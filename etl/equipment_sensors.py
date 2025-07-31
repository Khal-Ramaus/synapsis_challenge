from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, min, max
spark = SparkSession.builder.appName("Production Log").config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33").getOrCreate()

df_sensors = spark.read.option("header", True).csv("/app/equipment_sensors.csv")
df_sensors = df_sensors.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
date_range = df_sensors.agg(min("timestamp").alias("Min Date"), max("timestamp").alias("Max Date")).collect()[0]

# print(date_range["Min Date"])
# print(date_range["Max Date"])

# df_sensors.show(5)

df_sensors.write.format("jdbc") \
    .option("url", "jdbc:mysql://synapsis_mysql:3306/coal_mining") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "equipment_sensors") \
    .option("user", "user") \
    .option("password", "password") \
    .mode("append") \
    .save()

# Tutup Spark Session
spark.stop()