from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("dupe-check")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

a = spark.read.format("delta").load("/tmp/delta/risk_alerts")
dupes = a.groupBy("window", "card_id").count().filter("count > 1")
print("duplicate rows:", dupes.count())
dupes.show(truncate=False)
