import requests
import pandas as pd
import mysql.connector
from datetime import datetime, timedelta

# ---------- TODAY ----------
today = datetime.today().date()
yesterday = today - timedelta(days=1)

# ---------- CITIES ----------
CITIES = {
    "Delhi": (28.6139, 77.2090),
    "Mumbai": (19.0760, 72.8777),
    "Bangalore": (12.9716, 77.5946)
}

# ---------- DB CONNECTION ----------
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="12345",
    database="weather_db"
)
cursor = conn.cursor()

# ---------- FUNCTION TO GET LAST DATE ----------
def get_last_date(city):
    query = """
    SELECT MAX(record_time) 
    FROM weather_data 
    WHERE city=%s AND data_type='historical'
    """
    cursor.execute(query, (city,))
    result = cursor.fetchone()[0]

    if result:
        return result.date() + timedelta(days=1)
    else:
        return today - timedelta(days=90)  # first time load


# ---------- INSERT FUNCTION ----------
def insert_data(df, city, dtype):

    insert_query = """
    INSERT IGNORE INTO weather_data
    (city, record_time, temperature, humidity, windspeed, data_type, extracted_at)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    """

    for _, row in df.iterrows():
        values = (
            city,
            row["record_time"].to_pydatetime(),
            float(row["temperature"]),
            float(row["humidity"]),
            float(row["windspeed"]),
            dtype,
            datetime.now()
        )
        cursor.execute(insert_query, values)

    conn.commit()


# ---------- LOOP EACH CITY ----------
for city, coords in CITIES.items():

    lat, lon = coords
    print(f"\nProcessing {city}...")

    # ===== GET START DATE FROM DB =====
    start_date = get_last_date(city)

    if start_date <= yesterday:

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = yesterday.strftime("%Y-%m-%d")

        print(f"Fetching historical from {start_str} to {end_str}")

        hist_url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date={start_str}&end_date={end_str}&hourly=temperature_2m,relativehumidity_2m,windspeed_10m&timezone=Asia%2FKolkata"

        hist_response = requests.get(hist_url, timeout=60)
        hist_data = hist_response.json()

        hist_df = pd.DataFrame({
            "record_time": hist_data["hourly"]["time"],
            "temperature": hist_data["hourly"]["temperature_2m"],
            "humidity": hist_data["hourly"]["relativehumidity_2m"],
            "windspeed": hist_data["hourly"]["windspeed_10m"]
        })

        hist_df["record_time"] = pd.to_datetime(hist_df["record_time"])

        insert_data(hist_df, city, "historical")
        print("Historical Updated")

    else:
        print("Historical already up-to-date")

    # ===== FORECAST (Always Latest) =====
    forecast_url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m,relativehumidity_2m,windspeed_10m&timezone=Asia%2FKolkata"

    forecast_response = requests.get(forecast_url, timeout=60)
    forecast_data = forecast_response.json()

    fc_df = pd.DataFrame({
        "record_time": forecast_data["hourly"]["time"],
        "temperature": forecast_data["hourly"]["temperature_2m"],
        "humidity": forecast_data["hourly"]["relativehumidity_2m"],
        "windspeed": forecast_data["hourly"]["windspeed_10m"]
    })

    fc_df["record_time"] = pd.to_datetime(fc_df["record_time"])

    insert_data(fc_df, city, "forecast")
    print("Forecast Updated")

cursor.close()
conn.close()

print("\nPIPELINE COMPLETED SUCCESSFULLY")