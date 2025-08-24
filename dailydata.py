import pandas as pd
import numpy as np
from polygon import RESTClient
import sqlite3
import pprint

from datetime import datetime, timedelta
import time


client = RESTClient(api_key="YOUR_POLYGON_API_KEY_HERE")

conn = sqlite3.connect('tradeapp.db')
c = conn.cursor()

def store_grouped_daily_data(response):
    """
    Store the response from the Polygon API to pull grouped daily data into the database.
    """
    if isinstance(response, list):
        c.execute('''
            CREATE TABLE IF NOT EXISTS grouped_daily_data (
                Ticker TEXT,
                Close REAL,
                High REAL,
                Low REAL,
                Transactions INTEGER,
                Open REAL,
                Timestamp INTEGER,
                Volume REAL,
                VWAP REAL,
                PRIMARY KEY (Ticker, Timestamp)
            )
        ''')
        conn.commit()

        for result in response:
            c.execute('''
                INSERT OR IGNORE INTO grouped_daily_data (Ticker, Close, High, Low, Transactions, Open, Timestamp, Volume, VWAP)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (result.ticker, result.close, result.high, result.low, result.transactions, result.open, result.timestamp, result.volume, result.vwap))
        conn.commit()
        print('ok')
    else:
        print(f"Failed to retrieve data: {response}")

c.execute("SELECT MAX(Timestamp) FROM grouped_daily_data")
last_timestamp = c.fetchone()[0]

if last_timestamp:
    # Convert milliseconds timestamp to datetime object
    last_datetime = datetime.fromtimestamp(last_timestamp / 1000)
    start_date = last_datetime + timedelta(days=1) # Start from the day after the last timestamp
    print(f"Starting data fetch from: {start_date.strftime('%Y-%m-%d')}")
else:
    start_date = datetime.strptime("2024-01-01", "%Y-%m-%d") # Default start date if table is empty
    print(f"No existing data found. Starting data fetch from default start date: {start_date.strftime('%Y-%m-%d')}")

end_date = datetime.now()
delta = end_date - start_date

if delta.days >= 0: # Only fetch if start_date is before end_date
    api_call_count = 0
    for i in range(delta.days + 1):
        date_str = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
        print(f"Fetching data for: {date_str}")
        grouped = client.get_grouped_daily_aggs(date_str)
        store_grouped_daily_data(grouped)
        api_call_count += 1
        if api_call_count % 5 == 0 and i != delta.days:
            print("Pausing for 60 seconds to respect API rate limits...")
            time.sleep(60)
else:
    print("Database is up to date.")

