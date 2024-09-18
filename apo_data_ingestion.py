import requests 
import sqlite3
import pandas as pd
from datetime import datetime 
import os 

API_KEY = "kcET3WMRllcQY6G4Cpyy0vU2beTM7nzH9iFngcTg"
APOD_API_URL = f"https://api.nasa.gov/planetary/apod?api_key={API_KEY}"
DB_PATH = "./nasa_data_pipeline/database/nasa_data.db"


def fetch_apod_data(date=None):
    """Fetch data from Nasa APOD API for a given date"""
    params = {}
    if date :
        params["date"] = date
    response = requests.get(APOD_API_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(F"Failed to fetch data: {response.status_code}")
        return None

def process_apod_data(data):
    """Process the raw APO API data into a structure format."""
    
    if data:
        apod_data = {
            "date": [data.get("date")],
            "title": [data.get("title")],
            "explanation": [data.get("explanation")],
            "url": [data.get("url")],
            "media_type": [data.get("media_type")]
        }
        df = pd.DataFrame([data])
        return df 
    else:
        return pd.DataFrame()


def create_database():
    """Create a SQLite database and the APOD table if it does not exist."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create table
    cursor.execute('''CREATE TABLE IF NOT EXISTS apod_data(
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        date TEXT NOT NULL,
                        title TEXT,
                        explanation TEXT,
                        url TEXT,
                        media_type TEXT
                    );           
                   ''')
    conn.commit()
    conn.close()


def insert_data_to_db(df):
    """Insert processed APOD data into the SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    df.to_sql("apo_data", conn, if_exists="append", index=False)
    conn.close()


def check_if_data_exists(date):
    """Check if the APOD data for a given date already exists in the database"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM apod_data WHERE date=?", (date,))
    data_exists = cursor.fetchone() is not None 
    conn.close()
    return data_exists


def run_pipeline():
    """Run the NASA APOD data ingestion pipeline."""
    create_database()
    
    today = datetime.now().strftime("%Y-%m-%d")
    
    if not check_if_data_exists(today):
        print(f"Fetching APOD data for {today}...")
        apod_data = fetch_apod_data()
        if apod_data:
            processed_data = process_apod_data(apod_data)
            
            insert_data_to_db(processed_data)
            print(f"Data successfully inserted into the database.")
        else:
            print("No data to process.")
    else:
        print(f"Data for {today} already exists in the database.")

if __name__ == "__main__":
    run_pipeline()        
        