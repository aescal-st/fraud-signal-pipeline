from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("verify")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("bronze count:", spark.read.format("delta").load("/tmp/delta/bronze_txns").count())
alerts = spark.read.format("delta").load("/tmp/delta/risk_alerts")
print("alerts count:", alerts.count())
alerts.show(10, truncate=False)
