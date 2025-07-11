import sqlite3
import requests
import json
import re

DB_FILE = "finance_data.db"
TICKER_URL = "https://www.sec.gov/files/company_tickers.json"

def setup_database():
    """
    Creates and safely updates the database schema.
    """
    print("Connecting to the database...")
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # --- Create 'companies' table if it doesn't exist ---
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS companies (
            id INTEGER PRIMARY KEY, ticker TEXT NOT NULL, name TEXT NOT NULL
        )
    ''')

    # --- Create 'analysis_results' table if it doesn't exist ---
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS analysis_results (
            video_id TEXT PRIMARY KEY, tickers TEXT, sentiment TEXT,
            summary TEXT, analysis_date DATE
        )
    ''')

    # --- Safely add the 'publish_date' column if it doesn't exist ---
    cursor.execute("PRAGMA table_info(analysis_results)")
    columns = [column[1] for column in cursor.fetchall()]
    if 'publish_date' not in columns:
        print("Adding 'publish_date' column to 'analysis_results' table...")
        cursor.execute("ALTER TABLE analysis_results ADD COLUMN publish_date TEXT")
    
    # --- Populate the 'companies' table (if empty) ---
    cursor.execute("SELECT COUNT(*) FROM companies")
    if cursor.fetchone()[0] == 0:
        print(f"Downloading company data from {TICKER_URL}...")
        headers = {'User-Agent': 'YourName YourEmail@example.com'}
        try:
            response = requests.get(TICKER_URL, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            print("Populating the 'companies' table...")
            companies_to_insert = [
                (v['ticker'], re.sub(r'\s+', ' ', v['title']).strip().upper())
                for k, v in data.items()
            ]
            cursor.executemany("INSERT INTO companies (ticker, name) VALUES (?, ?)", companies_to_insert)
        except requests.exceptions.HTTPError as e:
            print(f"Failed to download company data: {e}")

    conn.commit()
    conn.close()
    print(f"✅ Database schema in '{DB_FILE}' is up to date.")

if __name__ == "__main__":
    setup_database()