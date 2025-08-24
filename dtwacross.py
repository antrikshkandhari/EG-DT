import sqlite3
import pandas as pd
import numpy as np
from dtaidistance import dtw
from dtaidistance import dtw_ndim
from multiprocessing import Pool
import streamlit as st
import json

# Initialize database connection and ensure template bank table exists
conn = sqlite3.connect('tradeapp.db')
conn.execute(
    """
    CREATE TABLE IF NOT EXISTS template_bank (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT,
        data TEXT
    )
    """
)
conn.commit()

# --- Define MULTIPLE Template Patterns in a Dictionary ---
pattern_templates = {
  "Custom Pattern 1": [ # Added "Custom Pattern 1" template here
        [-0.00911162,  0.        ,  0.02404057,  0.06775701,  0.01816411, -0.01630197],
        [ 0.05977011,  0.23982869,  0.06666667,  0.23851204,  0.15947058, -0.07908127],
        [ 0.17570499,  0.0164076 ,  0.20758929, -0.01060071,  0.09373801,  0.01803571],
        [ 0.03505535, -0.02633815, -0.02016636, -0.025     , -0.03965971,  0.00272894],
        [-0.03743316,  0.0052356 , -0.00756475, -0.02197802, -0.01110523,  0.0138764 ]
    ],
    "Custom Pattern 2": [ # Added "Custom Pattern 1" template here
        [ 0.,          0.,          0.,          0.,          0.,          0.0219469 ],
        [-0.08384458, -0.02816901, -0.01725843,  0.04646018,  0.00365864, -0.01985201],
        [ 0.11830357,  0.13250518,  0.11017104,  0.14587738,  0.12870732, -0.03453875],
        [ 0.08782435,  0.07129799,  0.10813594,  0.01291513,  0.0646499,   0.01477231],
        [ 0.10550459,  0.21331058,  0.08178439,  0.29143898,  0.19985281, -0.05719323],
        [ 0.41410788,  0.20675105,  0.32989691,  0.13117066,  0.2305782,   0.02566085],
        [-0.11971831,  0.08391608, -0.06330749,  0.07605985,  0.03236159, -0.01599073],
        [ 0.15333333, -0.03225806,  0.09393103, -0.03012746, -0.00927932,  0.00516129],
        [-0.07052023, -0.07,       -0.11045265, -0.11708483, -0.09342462,  0.03209743],
        [-0.19029851, -0.0776583,  -0.102764,    0.,         -0.04920548, -0.01868742],
        [ 0.19047619,  0.13860104,  0.21169036,  0.18403248,  0.14543775, -0.05067429],
        [ 0.11419355,  0.02730375,  0.07301173, -0.03085714,  0.02936219,  0.00831368],
        [-0.00868558, -0.0166113,  -0.05224787, -0.05542453, -0.04247705,  0.02213483],
        [-0.08060748, -0.09931306, -0.02820513, -0.00873908, -0.04637671, -0.01667506],
        [-0.0184244,   0.06775359, -0.03627968,  0.06423174,  0.01499821, -0.06216568]
    ],
    "Custom Pattern 3": [ # Added "Custom Pattern 1" template here
        [0.0, 0.0, 0.0, 0.0, 0.0, -0.00031362],
        [0.01897395, -0.00356841, 0.02184744, -0.00248284, 0.00130654, 0.00348176],
        [-0.00364911, 0.02910927, 0.00066043, 0.02391301, 0.02369512, 0.00326314],
        [0.02391301, -0.00372466, 0.00934586, 0.00071362, 0.00031873, 0.00286703],
        [0.11052278, 0.16982863, 0.10058167, 0.16283633, 0.15638937, -0.00357762],
        [0.05105517, -0.00853202, 0.05572615, 0.00006062, 0.00388162, 0.00023699],
        [0.00215383, -0.01138944, -0.01607203, -0.02852769, -0.02887132, -0.00010665],
        [-0.03722997, -0.0339329, -0.0211616, -0.02731236, -0.02299333, 0.00422125]
    ],
    "Custom Pattern 4": [ # Added "Custom Pattern 1" template here
        [0.0,         0.0,         0.0,         0.0,         0.0,         0.00213644],
        [0.00191283,  0.00727434,  0.01263595,  0.01092184,  0.01203016,  0.00324775],
        [0.00038213, -0.00075753, -0.01789235, -0.0064946,  -0.00756606,  0.00217338],
        [0.14334824,  0.17916802,  0.13901086,  0.17298304,  0.16480889, -0.00598514],
        [0.0258615,   0.02621564,  0.04400194, -0.00204575,  0.01719617,  0.01332682],
        [-0.03875999, -0.06963539, -0.04463642, -0.03091129, -0.04648192, -0.00232911],
        [0.00639865,  0.00047248,  0.0056958,   0.01486591,  0.0095265,  -0.00764188],
        [0.01055856,  0.02158861,  0.01493644,  0.01138033,  0.0177227,  -0.00132797],
        [0.0013179,  -0.00984863, -0.01173768, -0.01935758, -0.01896642, -0.00093726]
    ]
    }

# Convert template lists to NumPy arrays for DTW
pattern_template_arrays = {name: np.array(template) for name, template in pattern_templates.items()}

# --- Data Retrieval and Preprocessing Functions ---
def get_data_for_target(ticker):
    query = f""" SELECT Timestamp, Open, High, Low, Close, VWAP FROM grouped_daily_data WHERE Ticker = '{ticker}' AND Timestamp IN (SELECT Timestamp FROM grouped_daily_data WHERE Ticker = '{ticker}' ORDER BY Timestamp DESC LIMIT 9) ORDER BY Timestamp ASC"""
    df = pd.read_sql_query(query, conn)
    df['Vdiff'] = (df['VWAP'] - df['Close'])/df['Close']
    # Calculate log returns for each column
    for column in ['Open', 'High', 'Low', 'Close', 'VWAP']:
        df[column] = np.log(df[column]/df[column].shift(1)).fillna(0)

    data1 = df[['Open', 'High', 'Low', 'Close', 'VWAP', 'Vdiff']].dropna().to_numpy()
    print(data1)
    return data1

def get_data_for_stock(ticker):
    best_data = None
    best_distance = float('inf')
    
    # Try different LIMIT values from 7 to 15
    for limit in range(7, 16):
        query = f""" SELECT Timestamp, Open, High, Low, Close, VWAP FROM grouped_daily_data WHERE Ticker = '{ticker}' AND Timestamp IN (SELECT Timestamp FROM grouped_daily_data WHERE Ticker = '{ticker}' ORDER BY Timestamp DESC LIMIT {limit}) ORDER BY Timestamp ASC"""
        df = pd.read_sql_query(query, conn)
        df['Vdiff'] = (df['VWAP'] - df['Close'])/df['Close']
        # Calculate log returns for each column
        for column in ['Open', 'High', 'Low', 'Close', 'VWAP']:
            df[column] = np.log(df[column]/df[column].shift(1)).fillna(0)

        data1 = df[['Open', 'High', 'Low', 'Close', 'VWAP', 'Vdiff']].dropna().to_numpy()
        if len(data1) >= 2:  # Only consider if we have enough data points
            if best_data is None:
                best_data = data1
            else:
                # Compare with current best and update if better
                current_distance = dtw_ndim.distance(best_data, data1)
                if current_distance < best_distance:
                    best_data = data1
                    best_distance = current_distance
    
    return best_data if best_data is not None else np.array([])

# --- DTW Distance Calculation Functions ---
def dtw_distance_to_template(target_ticker, template):
    """Calculate multivariate DTW distance between a ticker and a given template."""
    target_data = get_data_for_target(target_ticker)

    if target_data.shape[0] < 2 or template.shape[0] < 2: # Handle insufficient data
        return (target_ticker, float('inf'))

    distance = dtw_ndim.distance(target_data, template)
    return (target_ticker, distance)

def calculate_dtw_distances_to_selected_template(target_ticker, stock_list, selected_template_array):
    """Calculate DTW distances in parallel to a *specific* selected template."""
    with Pool(processes=8) as pool:
        results = pool.starmap(dtw_distance_to_template, [(stock, selected_template_array) for stock in stock_list])
    return results

def dtw_distance_multivariate(target_ticker, comparison_ticker):
    """Calculate multivariate DTW distance between two tickers."""
    target_data = get_data_for_target(target_ticker)
    comparison_data = get_data_for_stock(comparison_ticker)
    
    if target_data.shape[0] < 2 or comparison_data.shape[0] < 2: # Handle cases with insufficient data
        return (comparison_ticker, float('inf')) # Return infinite distance

    distance = dtw_ndim.distance(target_data, comparison_data)
    return (comparison_ticker, distance)

def calculate_dtw_distances_to_stocks(target_ticker, stock_list):
    """Calculate DTW distances in parallel to other stocks (ticker vs. ticker comparison)."""
    with Pool(processes=8) as pool:
        results = pool.starmap(dtw_distance_multivariate, [(target_ticker, stock) for stock in stock_list])
    return results

def dtw_distance_to_templates(ticker):
    """Calculate distance to all templates and return the best match."""
    data = get_data_for_target(ticker)
    if data.shape[0] < 2:
        return (ticker, None, float('inf'))
    best_template = None
    best_distance = float('inf')
    for name, template in pattern_template_arrays.items():
        if template.shape[0] < 2:
            continue
        dist = dtw_ndim.distance(data, template)
        if dist < best_distance:
            best_distance = dist
            best_template = name
    return (ticker, best_template, best_distance)

def calculate_dtw_distances_to_all_templates(stock_list):
    """Calculate DTW distances for each stock to all templates and return best matches."""
    with Pool(processes=8) as pool:
        results = pool.map(dtw_distance_to_templates, stock_list)
    return results

def save_pattern_to_bank(ticker):
    """Save the current pattern for a ticker to the template bank table."""
    data = get_data_for_target(ticker)
    if data.size == 0:
        st.error(f"No data available for {ticker}")
        return
    conn.execute(
        "INSERT INTO template_bank (ticker, data) VALUES (?, ?)",
        (ticker, json.dumps(data.tolist())),
    )
    conn.commit()
    st.success(f"{ticker} added to template bank.")

# --- Helper Functions ---
def get_all_tickers():
    """Retrieve all unique stock tickers from the database based on volume and price criteria."""
    # First get top 2000 tickers by rank from rolling_returns
    rank_query = "SELECT DISTINCT Ticker FROM rolling_returns WHERE Date = (SELECT MAX(Date) FROM rolling_returns) ORDER BY Rank DESC LIMIT 2000"
    df_rank = pd.read_sql_query(rank_query, conn)
    rank_tickers = df_rank['Ticker'].tolist()
    
    # Then filter by volume and price criteria
    query = f"SELECT DISTINCT Ticker FROM grouped_daily_data WHERE Timestamp = (SELECT MAX(Timestamp) FROM grouped_daily_data) AND Volume > 100000 AND Close > 5 AND Ticker IN ({','.join(['?']*len(rank_tickers))})"
    df1 = pd.read_sql_query(query, conn, params=rank_tickers)
    
    # Filter by Close below MA4_High criteria
    ma_query = """
        SELECT Ticker FROM rolling_returns 
        WHERE Date = (SELECT MAX(Date) FROM rolling_returns) 
        AND Ticker IN ({})
    """.format(','.join(['?']*len(df1['Ticker'].tolist())))
    
    df_below_ma = pd.read_sql_query(ma_query, conn, params=df1['Ticker'].tolist())
    
    return df_below_ma['Ticker'].tolist()


# --- Main Streamlit App ---
def main():
    st.title("Stock Pattern Scanner")

    addticker = st.text_input("Enter Ticker to Analyze (Optional for Template Mode):", "") # Modified text input hint
    target_ticker = addticker.strip().upper()

    compare_mode = st.radio("Compare against:", ("Similar Stocks", "Template Pattern"))

    if compare_mode == "Template Pattern":
        stock_list = get_all_tickers() # Stock list is needed for template comparison
        if not stock_list:
            st.error("No tickers found in database based on criteria. Please check your database and criteria.")
            return
        st.subheader("Best Matches to Template Patterns (DTW)")
        results = calculate_dtw_distances_to_all_templates(stock_list)

    elif compare_mode == "Similar Stocks": # For similar stocks mode, ticker is required
        if not target_ticker:
            st.warning("Please enter a ticker to analyze for 'Similar Stocks' mode.")
            return

        stock_list = get_all_tickers()
        if not stock_list:
            st.error("No tickers found in database based on criteria. Please check your database and criteria.")
            return

        st.subheader(f"Stocks Similar to {target_ticker} (Multivariate DTW)")
        results = calculate_dtw_distances_to_stocks(target_ticker, stock_list)

    else:
        st.warning("Please select a comparison mode.")
        return # Exit if no mode selected

    if 'results' in locals(): # Proceed only if results are calculated in either mode
        if compare_mode == 'Template Pattern':
            valid_results = [
                (ticker, template, distance)
                for ticker, template, distance in results
                if distance != float('inf')
            ]
            results_sorted = sorted(valid_results, key=lambda x: x[2])
        else:
            valid_results = [
                (ticker, distance) for ticker, distance in results if distance != float('inf')
            ]
            results_sorted = sorted(valid_results, key=lambda x: x[1])

        if not results_sorted:
            st.warning("No similar patterns found for this ticker with valid distances.")
            return

        if compare_mode == 'Template Pattern':
            df = pd.DataFrame(results_sorted[:100], columns=['Ticker', 'Template', 'Distance'])
        else:
            df = pd.DataFrame(results_sorted[:100], columns=['Ticker', 'Distance'])

        file_name = f"similar_patterns" # Base filename
        if compare_mode == 'Similar Stocks':
            file_name += f"_{target_ticker}_Similar_Stocks"
        elif compare_mode == 'Template Pattern':
            file_name += "_All_Templates"
        file_name += ".xlsx"

        df.to_excel(file_name, index=False)
        st.success(f"Top {len(df)} similar patterns (or fewer) saved to {file_name}")

        st.write("Top Similar Patterns:")
        st.dataframe(df)

        if st.checkbox("Show Finviz Charts and Company Data for Top Patterns"):
            tick = df['Ticker'].tolist()

            # Helper function to get company data from stock_data table
            def get_company_data_from_db(ticker):
                try:
                    query = "SELECT * FROM stock_data WHERE Ticker = ?"
                    company_df = pd.read_sql_query(query, conn, params=(ticker,))
                    if not company_df.empty:
                        # Drop the Ticker column for display if desired
                        return company_df.drop(columns=["Ticker"], errors="ignore")
                    else:
                        return None
                except Exception as e:
                    return None

            for i in tick:
                st.subheader(f"{i}")
                st.image(f'https://charts2.finviz.com/chart.ashx?t={i}', width=900)
                # Display company data from stock_data table
                company_info = get_company_data_from_db(i)
                if company_info is not None and not company_info.empty:
                    # Adjust column width to fit column names using st.dataframe and set width to 'fit_columns'
                    st.dataframe(company_info, use_container_width=True, hide_index=True)
                else:
                    st.info(f"No company data found for {i} in stock_data table.")

                if st.button(f"Add {i} to Template Bank", key=f"add_{i}"):
                    save_pattern_to_bank(i)

                st.markdown("---")


if __name__ == '__main__':
    main()
