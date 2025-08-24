import yfinance as yf
import sqlite3
from datetime import datetime
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import time  # Import the time module

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

url = 'https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt'
DB_NAME = 'tradeapp.db'
# Removed API key as it's not used and should not be in code.

def get_tickers():
    try:
        response = requests.get(url)
        response.raise_for_status()
        tickers = set(ticker.strip() for ticker in response.text.split('\n') if ticker.strip())
        logging.info(f"Retrieved {len(tickers)} unique tickers")
        return list(tickers)
    except requests.RequestException as e:
        logging.error(f"Failed to retrieve tickers: {e}")
        return []

def create_table(conn):
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stock_data (
        Ticker TEXT,
        short_interest REAL,
        Industry TEXT,
        Sector TEXT,
        market_cap INTEGER,
        company_name TEXT,
        summary TEXT,
        analyst_opinions INTEGER,
        share_float INTEGER,
        revenue_growth REAL,
        earnings_growth REAL,
        PRIMARY KEY (Ticker)
    )
    ''')
    conn.commit()

def safe_get(info, key, default=None):
    try:
        value = info.get(key, default)
        return value if value is not None else default
    except:
        return default

def ticker_exists(conn, ticker_symbol):
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM stock_data WHERE Ticker = ?", (ticker_symbol,))
    return cursor.fetchone() is not None

def insert_stock_data(ticker_symbol):
    try:
        with sqlite3.connect(DB_NAME) as conn:
            if ticker_exists(conn, ticker_symbol):
                logging.info(f"Ticker {ticker_symbol} already exists in database. Skipping.")
                return ticker_symbol, None

            tick = yf.Ticker(ticker_symbol)
            time.sleep(10)  # Add a delay to avoid rate limiting
            info = tick.info

            data = {
                'Ticker': ticker_symbol,
                'short_interest': safe_get(info, 'shortPercentOfFloat', 0) * 100,
                'Industry': safe_get(info, 'industry', ''),
                'Sector': safe_get(info, 'sector', ''),
                'market_cap': safe_get(info, 'marketCap', 0),
                'company_name': safe_get(info, 'shortName', ''),
                'summary': safe_get(info, 'longBusinessSummary', ''),
                'analyst_opinions': safe_get(info, 'numberOfAnalystOpinions', 0),
                'share_float': safe_get(info, 'floatShares', 0),
                'revenue_growth': safe_get(info, 'revenueGrowth', 0) * 100,
                'earnings_growth': safe_get(info, 'earningsGrowth', 0) * 100
            }

            columns = ', '.join(data.keys())
            placeholders = ':' + ', :'.join(data.keys())

            cursor = conn.cursor()
            cursor.execute(f'''
            INSERT OR REPLACE INTO stock_data
            ({columns})
            VALUES ({placeholders})
            ''', data)
            conn.commit()

        logging.info(f"Data for {ticker_symbol} inserted successfully.")
        return ticker_symbol, None
    except Exception as e:
        logging.error(f"Error inserting data for {ticker_symbol}: {str(e)}")
        return ticker_symbol, str(e)

def main():
    tickers = get_tickers()
    if not tickers:
        return

    with sqlite3.connect(DB_NAME) as conn:
        create_table(conn)

    # Use ThreadPoolExecutor for concurrent execution
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_ticker = {executor.submit(insert_stock_data, ticker): ticker for ticker in tickers}

        for future in as_completed(future_to_ticker):
            ticker, error = future.result()
            if error:
                logging.error(f"Failed to process {ticker}: {error}")
            else:
                logging.info(f"Successfully processed {ticker}")

if __name__ == "__main__":
    main()