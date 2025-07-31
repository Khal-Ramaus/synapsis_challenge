from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, ArrayType, LongType, MapType
import requests

spark = SparkSession.builder.appName("Weather API").config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33").getOrCreate()

start_date = "2025-04-29"
end_date = "2025-08-15"
url = (
    f"https://api.open-meteo.com/v1/forecast?latitude=2.0167&longitude=117.3000&daily=temperature_2m_mean,precipitation_sum&timezone=Asia/Jakarta&past_days=0&start_date={start_date}&end_date={end_date}"
)

response = requests.get(url)
data = response.json()
# print(data)
# print(data)

# metadata
latitude = data.get("latitude")
longitude = data.get("longitude")
generationtime_ms = data.get("generationtime_ms")
utc_offset_seconds = data.get("utc_offset_seconds")
timezone = data.get("timezone")
timezone_abbreviation = data.get("timezone_abbreviation")
elevation = data.get("elevation")

# Daily
daily = data.get("daily", {})
dates = daily.get("time", [])
temps = daily.get("temperature_2m_mean", [])
precips = daily.get("precipitation_sum", [])

# data Daily + metadata
rows = [
    (
        date,
        temps[i],
        precips[i],
        latitude,
        longitude,
        generationtime_ms,
        utc_offset_seconds,
        timezone,
        timezone_abbreviation,
        elevation
    )
    for i, date in enumerate(dates)
]

# schema
schema = StructType([
    StructField("date", StringType(), True),
    StructField("temperature_2m_mean", DoubleType(), True),
    StructField("precipitation_sum", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("generationtime_ms", DoubleType(), True),
    StructField("utc_offset_seconds", LongType(), True),
    StructField("timezone", StringType(), True),
    StructField("timezone_abbreviation", StringType(), True),
    StructField("elevation", DoubleType(), True)
])

# create dataframe
df = spark.createDataFrame(rows, schema=schema)
# df.printSchema()
# df.show(truncate=False)

df.write.format("jdbc") \
    .option("url", "jdbc:mysql://synapsis_mysql:3306/coal_mining") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "weather_data") \
    .option("user", "user") \
    .option("password", "password") \
    .mode("append") \
    .save()

# Tutup Spark Session
spark.stop()