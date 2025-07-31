#!/bin/sh

echo "Running equipment_sensors.py..."
python equipment_sensors.py

echo "Running weather_api.py..."
python weather_api.py

echo "Running calculate_daily_metrics.py..."
python calculate_daily_metrics.py

echo "All ETL scripts finished."

# Jaga agar kontainer tetap berjalan di latar belakang setelah skrip selesai
# Ini berguna untuk melihat log nanti jika diperlukan
tail -f /dev/null