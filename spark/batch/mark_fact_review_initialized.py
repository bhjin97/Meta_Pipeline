from common.spark_session import create_spark_session


INITIAL_LOAD_MARKER_PATH = (
    "s3a://ecommerce/state/fact_review/"
    "_INITIAL_LOAD_SUCCESS"
)


def main():
    spark = create_spark_session(
        "Mark Fact Review Initialized"
    )

    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    marker_path = jvm.org.apache.hadoop.fs.Path(
        INITIAL_LOAD_MARKER_PATH
    )
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(
        jvm.java.net.URI(INITIAL_LOAD_MARKER_PATH),
        hadoop_conf,
    )

    if fs.exists(marker_path):
        print(
            "[INFO] fact_review already initialized. "
            f"marker={INITIAL_LOAD_MARKER_PATH}"
        )
        spark.stop()
        return

    parent_path = marker_path.getParent()
    fs.mkdirs(parent_path)

    output_stream = fs.create(
        marker_path,
        False,
    )
    output_stream.close()

    print(
        "[SUCCESS] fact_review initial load marked "
        "as complete. "
        f"marker={INITIAL_LOAD_MARKER_PATH}"
    )

    spark.stop()


if __name__ == "__main__":
    main()
