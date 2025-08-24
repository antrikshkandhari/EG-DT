import sqlite3
import pandas as pd
import time
import logging

def calculate_indicators():
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    start_time = time.time()
    logging.info("Starting calculation of indicators")
    
    # Connect to the database
    conn = sqlite3.connect('tradeapp.db')
    cur = conn.cursor()

    # Check if the grouped_daily_data table exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='grouped_daily_data'")
    if not cur.fetchone():
        logging.error("Error: grouped_daily_data table not found")
        conn.close()
        return

    # Load the data from grouped_daily_data table - process in chunks to avoid memory issues
    logging.info("Retrieving data from database")
    
    # Get unique tickers first
    cur.execute("SELECT DISTINCT Ticker FROM grouped_daily_data")
    tickers = [row[0] for row in cur.fetchall()]
    logging.info(f"Found {len(tickers)} unique tickers to process")
    
    # Create the new table if it doesn't exist
    cur.execute('''
        CREATE TABLE IF NOT EXISTS rolling_returns (
            Date DATE,
            Ticker TEXT,
            Close REAL,
            Volume REAL,
            VWAP REAL,
            RollingReturn1 REAL,
            RollingReturn7 REAL,
            RollingReturn25 REAL,
            MA4_High REAL,
            Rank REAL,
            PRIMARY KEY (Date, Ticker)
        )
    ''')
    
    # Process tickers in batches
    batch_size = 100
    total_processed = 0
    
    for i in range(0, len(tickers), batch_size):
        batch_tickers = tickers[i:i+batch_size]
        placeholders = ','.join(['?'] * len(batch_tickers))
        
        logging.info(f"Processing batch {i//batch_size + 1} of {(len(tickers) + batch_size - 1)//batch_size}")
        
        query = f"""
            SELECT Ticker, Timestamp, Close, High, Volume, VWAP 
            FROM grouped_daily_data 
            WHERE Ticker IN ({placeholders})
        """
        
        df = pd.read_sql_query(query, conn, params=batch_tickers)
        
        if df.empty:
            logging.warning(f"No data found for batch {i//batch_size + 1}")
            continue
            
        # Convert timestamp to datetime
        df['Date'] = pd.to_datetime(df['Timestamp'], unit='ms')
        df.sort_values(by=['Ticker', 'Date'], inplace=True)

        # Calculate the 1-day, 7-day, and 25-day rolling returns
        df['RollingReturn1'] = df.groupby('Ticker')['Close'].pct_change(periods=1)
        df['RollingReturn7'] = df.groupby('Ticker')['Close'].pct_change(periods=7)
        df['RollingReturn25'] = df.groupby('Ticker')['Close'].pct_change(periods=25)
        
        # Calculate 4-day moving average on High
        df['MA4_High'] = df.groupby('Ticker')['High'].rolling(window=4).mean().reset_index(level=0, drop=True)

        # Calculate the Rank
        df['Rank'] = ((df['RollingReturn1'] * 0.40) + (df['RollingReturn7'] * 0.35) + (df['RollingReturn25'] * 0.25))

        # Remove rows with NaN values
        df.dropna(subset=['RollingReturn1', 'RollingReturn7', 'RollingReturn25', 'Rank', 'MA4_High'], inplace=True)
        
        # Reset the index for inserting into the database
        df.reset_index(drop=True, inplace=True)

        # Insert the rolling returns into the new table
        df_to_insert = df[['Date', 'Ticker', 'Close', 'Volume', 'VWAP', 'RollingReturn1', 'RollingReturn7', 'RollingReturn25', 'MA4_High', 'Rank']]
        
        # Use a temporary table to avoid duplicate key conflicts
        temp_table_name = f'rolling_returns_temp_{i}'
        df_to_insert.to_sql(temp_table_name, conn, if_exists='replace', index=False)
        
        # Insert or replace data from the temporary table into the main table
        cur.execute(f'''
            INSERT OR REPLACE INTO rolling_returns
            SELECT Date, Ticker, Close, Volume, VWAP, RollingReturn1, RollingReturn7, RollingReturn25, MA4_High, Rank 
            FROM {temp_table_name}
        ''')

        conn.commit()

        # Drop the temporary table
        cur.execute(f'DROP TABLE IF EXISTS {temp_table_name}')
        conn.commit()
        
        total_processed += len(batch_tickers)
        elapsed_time = time.time() - start_time
        logging.info(f"Processed {total_processed}/{len(tickers)} tickers in {elapsed_time:.2f} seconds")

    logging.info(f"Successfully calculated rolling returns for {total_processed} tickers")
    logging.info(f"Total execution time: {time.time() - start_time:.2f} seconds")
    
    # Close the connection
    conn.close()

if __name__ == "__main__":
    calculate_indicators()
