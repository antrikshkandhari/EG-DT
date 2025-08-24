
## Core Files

- **`dailydata.py`**: Automated daily stock data collection from Polygon API
- **`companydata.py`**: Company data collection and storage using yfinance
- **`dtw.py`**: Main Streamlit application for pattern scanning and analysis
- **`dtwacross.py`**: Compare stocks against all templates and archive new patterns
- **`rollingretun.py`**: Technical indicator calculations and rolling returns


## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up your API keys:
   - **Polygon API key**: Replace `YOUR_POLYGON_API_KEY_HERE` in `dailydata.py`
   - **OpenAI API key**: Replace `YOUR_OPENAI_API_KEY_HERE` in any files that use it

3. Run the data collection scripts:
```bash
python dailydata.py      # Collect daily data
python rollingretun.py   # Calculate indicators
python companydata.py    # Collect company data
```

4. Launch the main application:
```bash
streamlit run dtw.py
```

   To analyze all templates simultaneously:
```bash
streamlit run dtwacross.py
```

## Dependencies

- pandas
- numpy
- streamlit
- dtaidistance
- polygon-api-client
- yfinance
- sqlite3
- multiprocessing

## Database

Uses SQLite with tables for:
- `grouped_daily_data`: Daily OHLCV data
- `rolling_returns`: Technical indicators and rankings
- `stock_data`: Company information and metrics

## License

For educational and research purposes. Ensure compliance with financial regulations and API terms of service.
