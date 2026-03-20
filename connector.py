import requests
import json
import os
import time
import logging
from tqdm import tqdm
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Data validation
def validate_data(df):
    logging.info("validating data")
    required_columns = ['period', 'outage', 'capacity'] 

    missing_cols = [col for col in required_columns if col not in df.columns] # verify if columns are complete
    if missing_cols:
        logging.error(f"Error: Missing required columns: {missing_cols}")
        return None
    cols_to_fix = ['outage', 'capacity'] # convert into int (before comparte to 0)
    for col in cols_to_fix:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    initial_count = len(df)
    df = df.dropna(subset=required_columns) # eliminate columns with NULL in required_columns 
    dropped_count = initial_count - len(df)

    if dropped_count > 0:
        logging.warning(f"Dropped {dropped_count} rows due to missing required values.")
    
    df = df[df['capacity'] >= 0] #capacity can't be negative
    logging.info("validate finished")
    return df

def save_to_parquet(df):
    try:
        logging.info("Saving data to Parquet file.")
        cols_num = ["capacity", "outage", "percentOutage"]
        for col in cols_num:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce') # Convert to numeric if not numeric values, set to NaN
        
        file_name = "nuclear_outages.parquet"
        df.to_parquet(file_name, engine='pyarrow', compression='snappy', index=False)
        logging.info(f"Data saved into {file_name} file")

    except Exception as e:
        logging.error(f"Error into saving parquet file: {e}")


def fetch_nuclear_outages():
    api_key = os.getenv("EIA_API_KEY") 
    if not api_key:
        logging.error("Error: La variable de entorno 'EIA_API_KEY' no está configurada.")
        return

    url = "https://api.eia.gov/v2/nuclear-outages/us-nuclear-outages/data/"
    all_data = []
    offset = 0
    length = 5000  # The API can only return 5000 rows in JSON format
  
    params = {
        "api_key": api_key,
        "frequency": "daily",
        "data[]": ["capacity", "outage", "percentOutage"],
        "offset": offset,
        "length": 0 # Get the total of records (for the progress bar, because I want to know how much are I'm gonna wait) 
    }
    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status() # Check if the request was successful, if not it will raise an HTTPError exception
        total = int(response.json()['response']['total'])
        logging.info(f"Total records: {total}")
        
        #  Download data
        with tqdm(total=total, desc="Download: ") as pbar:
            while offset < total:
                params["offset"] = offset
                params["length"] = length 
                
                try:
                    res = requests.get(url, params=params, timeout=15) # timeout of 10 seconds, if the server doesn't respond, it will raise a Timeout exception
                    res.raise_for_status()
                    data = res.json()['response']['data']
                    
                    if not data:
                        break

                    all_data.extend(data)
                    offset += len(data)
                    pbar.update(len(data))
                    time.sleep(0.2)
                         # little break for the server
                except requests.exceptions.RequestException as e:
                    logging.warning(f"Network fail, retriying in 15 seconds... {e}")
                    time.sleep(15)   # if fail wait 15 seconds after retry (be patien to your computer/network)
                    continue

        if all_data:
            # validate after saving
            df = pd.DataFrame(all_data)
            df = validate_data(df)
            if df is not None and not df.empty:
                save_to_parquet(df)
            else:
                logging.error("No valid data to save.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching nuclear outages: {e}")

if __name__ == "__main__":
    fetch_nuclear_outages()