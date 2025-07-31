CREATE TABLE IF NOT EXISTS equipment_sensors (
    timestamp DATETIME,
    equipment_id VARCHAR(50),
    status VARCHAR(50),
    fuel_consumption FLOAT,
    maintenance_alert VARCHAR(5)
);