from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = (
        SparkSession.builder
        .appName("Kafka Streaming Test")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "order-events")
        .option("startingOffsets", "earliest")
        .load()
    )

    result = (
        df.selectExpr("CAST(value AS STRING)")
    )

    query = (
        result.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()