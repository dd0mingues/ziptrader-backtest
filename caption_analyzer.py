import re
import sqlite3
import logging
from datetime import date, datetime
from transformers import pipeline
import json

# --- NLP Models ---
logging.info("Loading NLP models (Sentiment, NER, Summarizer)...")
sentiment_analyzer = pipeline("sentiment-analysis", model="yiyanghkust/finbert-tone")
summarizer = pipeline("summarization", model="facebook/bart-large-cnn")
logging.info("✅ Models loaded successfully.")


def _get_numerical_sentiment(result: dict) -> float:
    label = result.get('label', 'Neutral')
    score = result.get('score', 1.0)
    
    if label == 'Positive':
        return 1.0 * score
    elif label == 'Negative':
        return -1.0 * score
    else: # Neutral
        return 0.0


def analyze_text(text: str, db_file: str) -> dict:
    if not text or not text.strip():
        logging.warning("Skipping analysis because transcript text is empty.")
        return {"summary": "", "stocks": []}
    
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT ticker, name FROM companies")
        target_companies = [{"ticker": row[0], "name": row[1]} for row in cursor.fetchall()]
        conn.close()
    except sqlite3.Error as e:
        logging.error(f"🚨 Database Error: Could not fetch target companies. Error: {e}")
        return {"summary": "", "stocks": []} # Exit if we can't get companies

    main_summary = ""
    try:
        summary_result = summarizer(text, max_length=300, min_length=75, do_sample=False)
        main_summary = summary_result[0]['summary_text'] if summary_result else ""
    except Exception as e:
        logging.error(f"💥 Main summarization failed. Error: {e}")


    stock_analysis_results = []
    padded_text = " " + text + " "
    sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s', padded_text)

    for company in target_companies:
        ticker = company['ticker']
        try:
            clean_name = re.sub(r'\b(Inc|LLC|Corp|Corporation)\b', '', company['name'], flags=re.IGNORECASE).strip()
            ticker_pattern = r'\b' + re.escape(ticker) + r'\b'
            name_pattern = r'\b' + re.escape(clean_name) + r'\b'
            
            mentions = [s.strip() for s in sentences if re.search(ticker_pattern, s, re.IGNORECASE) or re.search(name_pattern, s, re.IGNORECASE)]

            if not mentions:
                continue

            context_block = ". ".join(mentions).strip()
            if not context_block:
                continue

            explanation = ""
            if len(context_block) < 100:
                explanation = context_block
            else:
                try:
                    explanation_result = summarizer(context_block, max_length=120, min_length=20, do_sample=False)
                    if explanation_result:
                        explanation = explanation_result[0]['summary_text']
                except Exception as e:
                    # Log the summarization-specific error but continue
                    logging.warning(f"⚠️ Context summarization for '{ticker}' failed. Using raw context instead. Error: {e}")
                    explanation = context_block # Fallback to using the full context

            if explanation:
                try:
                    sentiment_list = sentiment_analyzer(explanation)
                    if sentiment_list:
                        numerical_sentiment = _get_numerical_sentiment(sentiment_list[0])
                        stock_analysis_results.append({
                            "stock_name": ticker,
                            "sentiment": round(numerical_sentiment, 4),
                            "explanation": explanation
                        })
                except Exception as e:
                     # Log the sentiment-specific error and skip this stock
                    logging.error(f"💥 Sentiment analysis for '{ticker}' failed. Skipping stock. Error: {e}")
        
        except Exception as e:
            # A general catch-all for any other unexpected error for this company
            logging.error(f"🚨 An unexpected error occurred while processing stock '{ticker}': {e}")
            continue

    return {
        "summary": main_summary,
        "stocks": stock_analysis_results
    }


def save_analysis_to_db(video_id: str, publish_date: str, analysis: dict, db_file: str):
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        tickers_found = [stock['stock_name'] for stock in analysis.get("stocks", [])]
        tickers_str = ",".join(tickers_found)
        summary_text = analysis.get("summary", "")
        main_sentiment_label = "NEUTRAL"

        if summary_text:
            try:
                summary_sentiment_result = sentiment_analyzer(summary_text)[0]
                main_sentiment_label = summary_sentiment_result.get('label', 'Neutral').upper()
            except IndexError:
                logging.warning(f"⚠️ Could not determine sentiment for main summary of video {video_id}.")
            except Exception as e:
                logging.error(f"💥 Sentiment analysis for main summary of video {video_id} failed. Error: {e}")

        formatted_date = datetime.strptime(publish_date, "%Y%m%d").strftime("%Y-%m-%d")
        
        cursor.execute("""
            INSERT OR REPLACE INTO analysis_results (video_id, tickers, sentiment, summary, analysis_date, publish_date)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (video_id, tickers_str, main_sentiment_label, summary_text, date.today(), formatted_date))
        
        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        logging.error(f"🚨 Database Error: Could not save analysis for video {video_id}. Error: {e}")
    except Exception as e:
        logging.error(f"🚨 An unexpected error occurred during database save for video {video_id}. Error: {e}")