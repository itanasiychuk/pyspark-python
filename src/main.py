from pyspark.sql import SparkSession
from pipelines.data_pipeline import execute_pipeline


def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("Complete PySpark Pipeline").getOrCreate()

    # Execute the pipeline with the initialized Spark session
    execute_pipeline(spark)

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    main()
