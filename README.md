# Weather-Data-Pipeline-Open-Meteo-Python-MySQL-Power-BI
An end-to-end data engineering project that fetches real-time weather data from a public API, stores and analyzes it in a relational database, and visualizes insights through an interactive dashboard.

📌 Project Overview
This project demonstrates a complete ETL (Extract, Transform, Load) pipeline:

Extract – Fetch weather data (Temperature, Humidity, Wind Speed, Precipitation) from the Open-Meteo API
Transform & Load – Store structured data in MySQL and run analytical SQL queries
Visualize – Build an interactive Power BI dashboard from the MySQL data


🛠️ Tech Stack
LayerToolData SourceOpen-Meteo API (free, no API key needed)Data ExtractionPython (requests, pandas)DatabaseMySQL WorkbenchVisualizationMicrosoft Power BI

⚙️ How It Works
Step 1 — Fetch Data with Python
The Python script calls the Open-Meteo API and retrieves hourly weather data:

🌡️ Temperature
💧 Humidity
💨 Wind Speed
🌧️ Precipitation
📚 Key Learnings

Step 2 — Analyze in MySQL Workbench
Once data is loaded, SQL queries perform:

Aggregations — Average, max, and min values per day/week
Trend Analysis — Weather patterns over time
Extreme Values — Identifying hottest, coldest, and wettest periods

Step 3 — Visualize in Power BI
Power BI connects directly to MySQL and renders an interactive dashboard with:

Time-series trend charts
KPI cards (avg temperature, total precipitation, etc.)
Filters by date range

📚 Key Learnings

Consuming REST APIs and parsing JSON responses in Python
Designing a relational schema for time-series weather data
Writing analytical SQL queries (aggregations, trend analysis)
Building interactive dashboards in Power BI with a live MySQL connection
Understanding a full ETL pipeline from raw data to business insight


🔗 Resources

Open-Meteo API Docs
Power BI Desktop
MySQL Connector/Python

