from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Airflow Spark Test Job")
    .getOrCreate()
)

data = [
    ("order", 100),
    ("delivery", 80),
    ("review", 30),
]

df = spark.createDataFrame(data, ["event_type", "count"])

df.show()

spark.stop()