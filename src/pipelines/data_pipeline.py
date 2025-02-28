from pyspark.sql import functions as F


def process_data(df):
    # Step 1: Add a new column "age_in_5_years"
    df = df.withColumn("age_in_5_years", df.age + 5)

    # Step 2: Filter out people older than 30
    df = df.filter(df.age > 30)

    return df


def execute_pipeline(spark, input_path="data/input_data.csv"):
    # Step 1: Load data from CSV
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Step 2: Process data using the transformation function
    processed_df = process_data(df)

    # Step 3: Show the processed data
    processed_df.show()
pipelines