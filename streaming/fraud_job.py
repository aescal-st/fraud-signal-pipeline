#Spark job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = (
    SparkSession.builder.appName("fraud-signal-pipeline")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

#Declare shape of data

schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("card_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("event_time", TimestampType()),
])


#Read the stream

txns = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "transactions")
    .option("startingOffsets", "latest")
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("t"))
    .select("t.*")
    .withWatermark("event_time", "10 minutes")
)


#Fraud Detection
windowed = (
    txns.groupBy(window(col("event_time"), "5 minutes", "1 minute"), col("card_id"))
    .agg(count("*").alias("txn_count"), _sum("amount").alias("total_amt"))
)
alerts = windowed.filter((col("txn_count") > 4) | (col("total_amt") > 400))


#Writing the results
bronze_q = (
    txns.writeStream.format("delta").outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoints/bronze_txns")
    .start("/tmp/delta/bronze_txns")
)
alerts_q = (
    alerts.writeStream.format("delta").outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoints/risk_alerts")
    .start("/tmp/delta/risk_alerts")
)
spark.streams.awaitAnyTermination()

