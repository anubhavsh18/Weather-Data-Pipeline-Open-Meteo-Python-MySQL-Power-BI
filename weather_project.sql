SET SQL_SAFE_UPDATES = 0;

CREATE DATABASE weather_db;
USE weather_db;

USE weather_db;



-- Table 1: Cities (dimension table)
CREATE TABLE cities (
  city_id    INT AUTO_INCREMENT PRIMARY KEY,
  city_name  VARCHAR(100) NOT NULL,
  country    VARCHAR(100) NOT NULL,
  latitude   DECIMAL(7,4) NOT NULL,
  longitude  DECIMAL(7,4) NOT NULL,
  timezone   VARCHAR(50) NOT NULL
);

-- Table 2: Weather readings (fact table — main data)
CREATE TABLE weather_readings (
  reading_id         BIGINT AUTO_INCREMENT PRIMARY KEY,
  city_id            INT NOT NULL,
  recorded_at        DATETIME NOT NULL,
  temperature_c      DECIMAL(5,2),
  feels_like_c       DECIMAL(5,2),
  precipitation_mm   DECIMAL(6,2),
  rain_mm            DECIMAL(6,2),
  windspeed_kmh      DECIMAL(6,2),
  wind_direction_deg SMALLINT,
  humidity_pct       SMALLINT,
  cloudcover_pct     SMALLINT,
  FOREIGN KEY (city_id) REFERENCES cities(city_id)
);



-- Table 3: Daily summary (aggregated)
CREATE TABLE daily_summary (
  summary_id       BIGINT AUTO_INCREMENT PRIMARY KEY,
  city_id          INT NOT NULL,
  summary_date     DATE NOT NULL,
  avg_temp_c       DECIMAL(5,2),
  max_temp_c       DECIMAL(5,2),
  min_temp_c       DECIMAL(5,2),
  total_rain_mm    DECIMAL(6,2),
  avg_humidity_pct DECIMAL(5,2),
  avg_windspeed    DECIMAL(6,2),
  avg_cloudcover   DECIMAL(5,2),
  FOREIGN KEY (city_id) REFERENCES cities(city_id)
);

-- Table 4: Wind direction categories (lookup)
CREATE TABLE wind_direction_lookup (
  degree_min  SMALLINT,
  degree_max  SMALLINT,
  direction   VARCHAR(20)
);

-- Seed wind direction lookup
INSERT INTO wind_direction_lookup VALUES
  (0, 22, 'North'),
  (23, 67, 'Northeast'),
  (68, 112, 'East'),
  (113, 157, 'Southeast'),
  (158, 202, 'South'),
  (203, 247, 'Southwest'),
  (248, 292, 'West'),
  (293, 337, 'Northwest');

INSERT INTO cities (city_name, country, latitude, longitude, timezone) VALUES
  ('Mumbai',    'India',   19.0760,  72.8777, 'Asia/Kolkata'),
  ('Delhi',     'India',   28.6139,  77.2090, 'Asia/Kolkata'),
  ('Bangalore', 'India',   12.9716,  77.5946, 'Asia/Kolkata')
  
  Select * from weather_data;

SELECT MIN(record_time), MAX(record_time)
FROM weather_data;

SELECT DISTINCT city FROM weather_data;


-- check null values
SELECT *
FROM weather_data
WHERE temperature IS NULL
   OR humidity IS NULL
   OR windspeed IS NULL;
   
   
-- Checking duplicates
SELECT city, record_time, data_type, COUNT(*)
FROM weather_data
GROUP BY city, record_time, data_type
HAVING COUNT(*) > 1;

-- Removing duplicates
CREATE TEMPORARY TABLE dup_ids AS
SELECT w1.id
FROM weather_data w1
JOIN weather_data w2
ON w1.city = w2.city
AND w1.record_time = w2.record_time
AND w1.data_type = w2.data_type
AND w1.id > w2.id;

CREATE TABLE weather_data_clean LIKE weather_data;

INSERT INTO weather_data_clean
SELECT *
FROM weather_data w
WHERE w.id IN (
    SELECT MIN(id)
    FROM weather_data
    GROUP BY city, record_time, data_type
);


SELECT COUNT(*) FROM weather_data;
SELECT COUNT(*) FROM weather_data_clean;

ALTER TABLE weather_data
ADD CONSTRAINT unique_weather
UNIQUE (city, record_time, data_type);

SELECT city, record_time, data_type, COUNT(*)
FROM weather_data
GROUP BY city, record_time, data_type
HAVING COUNT(*) > 1;

SELECT COUNT(*) FROM weather_data;

ALTER TABLE weather_data
ADD record_date DATE,
ADD record_hour TINYINT;

UPDATE weather_data
SET 
    record_date = DATE(record_time),
    record_hour = HOUR(record_time);

SELECT city, record_time, record_date, record_hour
FROM weather_data
LIMIT 10;

SELECT MIN(temperature), MAX(temperature)
FROM weather_data;

SELECT MIN(humidity), MAX(humidity)
FROM weather_data;


ALTER TABLE weather_data
ADD heat_risk_level VARCHAR(20);

UPDATE weather_data
SET heat_risk_level =
CASE
    WHEN temperature >= 40 THEN 'Severe'
    WHEN temperature >= 35 THEN 'High'
    WHEN temperature >= 30 THEN 'Moderate'
    ELSE 'Normal'
END;

SELECT heat_risk_level, COUNT(*)
FROM weather_data
GROUP BY heat_risk_level;



CREATE VIEW vw_weather_clean AS
SELECT
    city,
    record_date,
    record_hour,
    temperature,
    humidity,
    windspeed,
    data_type,
    heat_risk_level
FROM weather_data;

ALTER TABLE weather_data
DROP COLUMN heatwave_flag;

SELECT * 
FROM vw_weather_clean
LIMIT 20;

SELECT DISTINCT heat_risk_level
FROM vw_weather_clean;