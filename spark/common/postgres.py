import os

from pyspark.sql import DataFrame, SparkSession


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "ecommerce")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

POSTGRES_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

JDBC_OPTIONS = {
    "url": POSTGRES_URL,
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}


def write_to_postgres(
    df: DataFrame,
    table_name: str,
    schema: str = "gold_staging",
    mode: str = "overwrite",
) -> None:
    (
        df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", f"{schema}.{table_name}")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode(mode)
        .save()
    )


def read_from_postgres(
    spark: SparkSession,
    table_name: str,
    schema: str = "gold_staging",
) -> DataFrame:
    return (
        spark.read
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", f"{schema}.{table_name}")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )