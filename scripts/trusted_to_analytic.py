
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, split, substring
from pyspark.sql.types import StringType, DateType


@udf(returnType=StringType())
def my_udf(value: str):
    return value


def apply_udf_to_columns(df, col_list, f):
    df_clean = df
    for column in col_list:
        df_clean = df_clean.withColumn(
            f"{column}_applied_udf",
            f(col(column))
        )
    return df_clean


def extract(spark, origin_bucket: str):
    return(
        spark
        .read
        .format("parquet")
        .option("header", "true")
        .load(origin_bucket)
    )


def transform(df):
    df_clean = df.withColumn(
        "date", 
        #substring(split(df.filepath, "/").getItem(8), 1, 10).cast(DateType()) # LOCAL filesystem
        substring(split(df.filepath, "/").getItem(3), 1, 10).cast(DateType()) # S3 filesystem
    )

    df_clean.printSchema()
    print((df_clean.count(), len(df_clean.columns)))
    
    return df_clean


def load(df, destination_bucket: str):
    (
        df
        .write
        .partitionBy("date")
        .mode("overwrite")
        .parquet(destination_bucket)
    )


if __name__ == "__main__":

    sc = SparkContext()
    (
        sc._jsc.hadoopConfiguration()
        .set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
    )
    
    spark = (
        SparkSession(sc)
        .builder
        .appName("howbootcamp_desafio2_trusted_to_analytic")
        .getOrCreate()
    )

    origin_bucket_url = f"s3://trusted-a8fe5d9/transactions/"
    # origin_bucket_url = "./../trusted/"
    destination_bucket_url = f"s3://analytic-a4c607a/transactions/"
    # destination_bucket_url = "./../analytic/"

    print("Starting data extraction...")
    df = extract(spark, origin_bucket_url)
    print("Data extraction done!")

    print("Starting data transformation...")
    df_clean = transform(df)
    print("Data transformation done!")
    
    print("Started loading data...")
    load(df_clean, destination_bucket_url)
    print("Data load done!")
