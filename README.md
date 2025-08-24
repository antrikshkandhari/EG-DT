# Core Trading Pattern Analysis

A focused collection of Python scripts for stock trading pattern analysis using Dynamic Time Warping (DTW) algorithms and technical indicators.

## Core Files

- **`dailydata.py`**: Automated daily stock data collection from Polygon API
- **`companydata.py`**: Company data collection and storage using yfinance
- **`dtw.py`**: Main Streamlit application for pattern scanning and analysis
- **`rollingretun.py`**: Technical indicator calculations and rolling returns

## Features

- **Pattern Recognition**: DTW algorithms for finding similar stock patterns
- **Data Collection**: Automated daily data collection and storage
- **Technical Analysis**: Rolling returns, moving averages, and rankings
- **Company Information**: Financial metrics and company data

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

## Dependencies

- pandas
- numpy
- streamlit
- dtaidistance
- polygon-api-client
- yfinance
- sqlite3
- multiprocessing

## Usage

The system provides pattern recognition capabilities:
- Compare stocks against predefined templates
- Find stocks similar to a target ticker
- Export results to Excel for further analysis

## Database

Uses SQLite with tables for:
- `grouped_daily_data`: Daily OHLCV data
- `rolling_returns`: Technical indicators and rankings
- `stock_data`: Company information and metrics

## License

For educational and research purposes. Ensure compliance with financial regulations and API terms of service.
