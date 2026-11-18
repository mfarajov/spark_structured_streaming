from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)

# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Week10_IOT_Temperature_Streaming")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# --------------------------------------------------
# 2. Define CSV Schema
# --------------------------------------------------

iot_schema = StructType([
    StructField("id", StringType(), True),
    StructField("room_id/id", StringType(), True),
    StructField("noted_date", StringType(), True),
    StructField("temp", IntegerType(), True),
    StructField("out/in", StringType(), True),
    StructField("gentime_time", StringType(), True)
])

# --------------------------------------------------
# 3. Read Streaming Data from RustFS
# --------------------------------------------------

df = (
    spark.readStream
    .format("csv")
    .schema(iot_schema)
    .option("header", "true")
    .option("maxFilesPerTrigger", 1)
    .load("s3a://datasets/iot-temp-input/")
)

# --------------------------------------------------
# 4. Rename Columns
# --------------------------------------------------

df_clean = (
    df
    .withColumnRenamed("room_id/id", "room_id_id")
    .withColumnRenamed("out/in", "out_in")
)

# --------------------------------------------------
# 5. Add Event-Time Columns
# --------------------------------------------------

df_enriched = (
    df_clean
    .withColumn(
        "noted_date_ts",
        F.to_timestamp(
            F.col("noted_date"),
            "dd-MM-yyyy HH:mm"
        )
    )
    .withColumn(
        "event_year",
        F.year("noted_date_ts")
    )
    .withColumn(
        "event_month",
        F.month("noted_date_ts")
    )
    .withColumn(
        "event_day",
        F.dayofweek("noted_date_ts")
    )
)

# --------------------------------------------------
# 6. Write Streaming Data to Console
# --------------------------------------------------

query = (
    df_enriched.writeStream
    .format("console")
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .option("numRows", 10)
    .option("truncate", True)
    .option(
        "checkpointLocation",
        "s3a://datasets/checkpoints/iot-temp-console"
    )
    .start()
)

# --------------------------------------------------
# 7. Keep Streaming Application Running
# --------------------------------------------------

query.awaitTermination()