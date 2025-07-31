from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc, sum, avg, when, lit, unix_timestamp, lead, coalesce, round, expr
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Daily Production Metrics").config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33").getOrCreate()

# Extract data from MYSQL Table
def read_table(table_name):
    print(f"Reading data from {table_name}...")
    df = spark.read.format("jdbc") \
        .option("url", "jdbc:mysql://synapsis_mysql:3306/coal_mining") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", table_name) \
        .option("user", "user") \
        .option("password", "password") \
        .load()
    print(f"Successfully read {table_name}.")
    return df

df_production_log = read_table("production_logs")
df_equipment_sensors = read_table("equipment_sensors")
df_weather_data = read_table("weather_data")

# to ensure date or timestamp is in correct format
df_production_log = df_production_log.withColumn("date", col("date").cast("date"))
df_equipment_sensors = df_equipment_sensors.withColumn("timestamp", col("timestamp").cast("timestamp"))
df_weather_data = df_weather_data.withColumn("date", col("date").cast("date"))

# If tons_extracted is negative, replace it with 0
df_production_log = df_production_log.withColumn(
    "tons_extracted", 
    when(col("tons_extracted")<0, 0.0).otherwise(col("tons_extracted").cast("double"))
)
print("Replace negative tons_extracted with 0")

# total_production_daily & average_quality_grade
df_daily_production_quality = df_production_log.groupBy("date") \
    .agg(
        # total_production_daily: Total tons of coal mined per day.
        sum("tons_extracted").alias("total_tons_mined_daily"),
        # average_quality_grade: Average coal quality per day
        avg("quality_grade").alias("average_quality_grade")
    )
df_daily_production_quality.show(5)
print("Calculated daily total production and average quality.")


# equipment_utilization: Percentage of time equipment is operational (status "active") per day.

# Extract duration
window_spec_eq=Window.partitionBy("equipment_id").orderBy("timestamp")
df_equipment_active_duration = df_equipment_sensors \
    .withColumn("next_timestamp", lead("timestamp", 1).over(window_spec_eq)) \
    .withColumn("end_time", coalesce(col("next_timestamp"),
        date_trunc("day", col("timestamp")) + expr("INTERVAL 1 DAY - INTERVAL 1 SECOND"))) \
    .withColumn("duration_seconds", unix_timestamp(col("end_time")) - unix_timestamp(col("timestamp"))) \
    .filter(col("status") == "active") 
df_equipment_active_duration.show(5)

# Total Active duration per day
df_daily_equipment_utilization_raw = df_equipment_active_duration \
    .withColumn("date", date_trunc("day", col("timestamp"))) \
    .groupBy("date", "equipment_id") \
    .agg(sum("duration_seconds").alias("total_active_seconds_per_day"))
df_daily_equipment_utilization_raw.show(5)

# Percentage of equipment_utilization
df_daily_equipment_utilization = df_daily_equipment_utilization_raw \
    .withColumn("total_seconds_in_day", lit(24 * 60 * 60)) \
    .withColumn("equipment_utilization_pct_single",
                (col("total_active_seconds_per_day") / col("total_seconds_in_day")) * 100) \
    .groupBy("date") \
    .agg(avg("equipment_utilization_pct_single").alias("equipment_utilization_pct"))
df_daily_equipment_utilization.show(5)

# Verify equipment_utilization is between 0 and 100%.
df_daily_equipment_utilization = df_daily_equipment_utilization.withColumn(
    "equipment_utilization_pct",
    when(col("equipment_utilization_pct") < 0, 0.0)
    .when(col("equipment_utilization_pct") > 100.0, 100.0)
    .otherwise(col("equipment_utilization_pct"))
)
df_daily_equipment_utilization.show()
print("Calculated daily equipment utilization and verified bounds (0-100%).")


# fuel_efficiency: Average fuel consumption per ton of coal mined.
df_daily_fuel_consumption = df_equipment_sensors \
    .withColumn("date", date_trunc("day", col("timestamp"))) \
    .groupBy("date") \
    .agg(sum("fuel_consumption").alias("total_fuel_consumed_daily"))
df_daily_fuel_consumption.show()

df_fuel_efficiency = df_daily_fuel_consumption.join(df_daily_production_quality, on="date", how="inner") \
    .withColumn("fuel_efficiency",
        when(col("total_tons_mined_daily") > 0,
            col("total_fuel_consumed_daily") / col("total_tons_mined_daily"))
        .otherwise(0.0))
df_fuel_efficiency.show()
print("Calculated daily fuel efficiency.")


# weather_impact: Correlation between rainfall and daily production (e.g., production on rainy vs. non-rainy days).
df_daily_weather_metrics = df_weather_data.groupBy("date") \
    .agg(
        sum("precipitation_sum").alias("daily_rainfall_mm"),
        avg("temperature_2m_mean").alias("average_temp_daily")
    ) \
    .withColumn("is_rainy_day", when(col("daily_rainfall_mm") > 0, lit(True)).otherwise(lit(False)))
df_daily_weather_metrics.show(5)
print("Calculated daily weather metrics.")


# Create final Dataframe from all dataframe
df_final_metrics = df_daily_production_quality
df_final_metrics = df_final_metrics.join(df_daily_equipment_utilization, on="date", how="outer")
df_final_metrics = df_final_metrics.join(df_fuel_efficiency.select("date", "total_fuel_consumed_daily", "fuel_efficiency"), on="date", how="outer")
df_final_metrics = df_final_metrics.join(df_daily_weather_metrics, on="date", how="outer")

# Replace Null value with 0. It means there is no data on that day
df_final_metrics = df_final_metrics.fillna(0.0, subset=[
    "total_tons_mined_daily",
    "average_quality_grade",
    "equipment_utilization_pct",
    "total_fuel_consumed_daily",
    "fuel_efficiency",
    "daily_rainfall_mm",
    "average_temp_daily"
])

df_final_metrics = df_final_metrics.na.fill(False, subset=["is_rainy_day"])
print("Combined all daily metrics. Missing weather data for production days are implicitly handled by filling with 0/False.")

# Round to 2 decimal places
df_final_metrics = df_final_metrics.withColumn("average_quality_grade", round(col("average_quality_grade"), 2)) \
                                   .withColumn("equipment_utilization_pct", round(col("equipment_utilization_pct"), 2)) \
                                   .withColumn("fuel_efficiency", round(col("fuel_efficiency"), 2)) \
                                   .withColumn("daily_rainfall_mm", round(col("daily_rainfall_mm"), 2)) \
                                   .withColumn("average_temp_daily", round(col("average_temp_daily"), 2))
print("Final combined daily metrics DataFrame")
df_final_metrics.orderBy("date").show(truncate=False)

# Load to MySQL
print("Saving daily metrics to daily_production_metrics table...")
df_final_metrics.write.format("jdbc") \
    .option("url", "jdbc:mysql://synapsis_mysql:3306/coal_mining") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "daily_production_metrics") \
    .option("user", "user") \
    .option("password", "password") \
    .mode("overwrite") \
    .save()

print("Daily production metrics successfully saved to MySQL.")

spark.stop()