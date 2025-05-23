import os
import mysql.connector
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from datetime import datetime, timedelta
from dotenv import load_dotenv
import numpy as np

# Load environment variables from .env file
load_dotenv()

# Get database credentials from environment variables
db_host = os.getenv('BI_HOST')
db_user = os.getenv('BI_USER')
db_pass = os.getenv('BI_PASS')
db_name = os.getenv('GEN_DB_NAME')

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

def fetch_weather_data(latitude, longitude, start_date, end_date):
    """
    Fetch weather data from Open-Meteo API for specified coordinates and date range
    Returns a DataFrame with date, temperature, and humidity
    """
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date.strftime('%Y-%m-%d'),
        "end_date": end_date.strftime('%Y-%m-%d'),
        "hourly": ["temperature_2m", "relative_humidity_2m"],
        "timezone": "America/Los_Angeles",
        "temperature_unit": "fahrenheit"
    }
    
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]
    
    # Process hourly data
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
    
    # Get hourly time as a correct array
    hourly_time = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        periods=len(hourly_temperature_2m),
        freq=pd.Timedelta(seconds=hourly.Interval())
    )
    
    # Create DataFrame
    hourly_data = {
        "datetime": hourly_time,
        "temperature": hourly_temperature_2m,
        "humidity": hourly_relative_humidity_2m
    }
    
    return pd.DataFrame(data=hourly_data)

def update_weather_data():
    """
    Main function to:
    1. Get rows with missing data from the database
    2. Fetch weather data from Open-Meteo
    3. Update the database with the fetched data
    """
    try:
        # Connect to the database
        conn = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_pass,
            database=db_name
        )
        
        cursor = conn.cursor(dictionary=True)
        
        # Execute query to get rows with missing temperature or humidity
        query = """
        SELECT id, `date`, `hour`, datetime, temperature, humidity
        FROM daniel1234.srp
        WHERE temperature IS NULL OR humidity IS NULL
        LIMIT 8000
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        if not rows:
            print("No rows with missing weather data found.")
            cursor.close()
            conn.close()
            return
        
        print(f"Found {len(rows)} rows with missing weather data.")
        
        # Get date range for weather API call
        datetimes = [datetime.strptime(str(row['datetime']), '%Y-%m-%d %H:%M:%S') 
                    for row in rows if row['datetime'] is not None]
        
        if not datetimes:
            print("No valid datetimes found in the data.")
            cursor.close()
            conn.close()
            return
        
        min_date = min(datetimes).date()
        max_date = max(datetimes).date()
        
        # Add a buffer day on each side to ensure we have all hours
        start_date = min_date - timedelta(days=1)
        end_date = max_date + timedelta(days=1)
        
        print(f"Fetching weather data from {start_date} to {end_date}")
        
        # Scottsdale, Arizona coordinates
        # You may want to make these configurable or fetch from your database
        latitude = 33.7591
        longitude = -111.7270
        
        # Fetch weather data
        weather_df = fetch_weather_data(latitude, longitude, start_date, end_date)
        
        # Convert timezone if needed - depends on your data
        # This assumes the database datetime is in the local timezone
        weather_df['datetime'] = weather_df['datetime'].dt.tz_convert('America/Los_Angeles')
        weather_df['datetime'] = weather_df['datetime'].dt.tz_localize(None)
        
        # Update database with weather data
        update_cursor = conn.cursor()
        updated_count = 0
        skipped_count = 0
        
        for row in rows:
            if row['datetime'] is None:
                continue
                
            # Find matching weather data
            row_datetime = datetime.strptime(str(row['datetime']), '%Y-%m-%d %H:%M:%S')
            
            # Find the closest timestamp in our weather data
            closest_weather = weather_df.iloc[(weather_df['datetime'] - row_datetime).abs().argsort()[:1]]
            
            if not closest_weather.empty:
                temperature_raw = closest_weather['temperature'].values[0]
                humidity_raw = closest_weather['humidity'].values[0]
                
                # Check for NaN values and handle them
                if pd.isna(temperature_raw) or pd.isna(humidity_raw):
                    print(f"Warning: NaN values found for datetime {row_datetime}, skipping row {row['id']}")
                    skipped_count += 1
                    continue
                
                # Convert to float (this will raise an error if still NaN somehow)
                try:
                    temperature = float(temperature_raw)
                    humidity = float(humidity_raw)
                    
                    # Additional validation - check if values are reasonable
                    if np.isnan(temperature) or np.isnan(humidity):
                        print(f"Warning: Invalid values (temp: {temperature}, humidity: {humidity}) for row {row['id']}, skipping")
                        skipped_count += 1
                        continue
                        
                except (ValueError, TypeError) as e:
                    print(f"Warning: Could not convert values to float for row {row['id']}: {e}")
                    skipped_count += 1
                    continue
                
                # Update the database
                update_query = """
                UPDATE daniel1234.srp 
                SET temperature = %s, humidity = %s
                WHERE id = %s
                """
                update_cursor.execute(update_query, (temperature, humidity, row['id']))
                updated_count += 1
            else:
                print(f"Warning: No weather data found for datetime {row_datetime}")
                skipped_count += 1
        
        conn.commit()
        print(f"Updated {updated_count} rows with weather data.")
        print(f"Skipped {skipped_count} rows due to missing or invalid data.")
        
        # Close cursor and connection
        update_cursor.close()
        cursor.close()
        conn.close()
        
    except mysql.connector.Error as err:
        print(f"Database Error: {err}")
    except Exception as e:
        print(f"Error: {e}")
        
if __name__ == "__main__":
    update_weather_data()
