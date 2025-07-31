CREATE TABLE IF NOT EXISTS weather_data (
    date DATE,
    temperature_2m_mean FLOAT,
    precipitation_sum FLOAT,
    latitude FLOAT,
    longitude FLOAT,
    generationtime_ms FLOAT,
    utc_offset_seconds FLOAT,
    timezone VARCHAR(15),
    timezone_abbreviation VARCHAR(5),
    elevation FLOAT
);