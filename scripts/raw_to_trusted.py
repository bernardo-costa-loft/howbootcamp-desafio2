
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType


@udf(returnType=StringType())
def clean_phones(value: str):
    s1 = value
    if "(0" in value:
        between_parentesis = value.split(")")[0].split("(")[1]
        if len(between_parentesis[1:]) < 2:
            s1 = value.replace("(","")
        else:
            s1 = value.replace("(0","")

    return(
        s1
        .replace("(","")
        .replace(")","")
        .replace("+","")
        .replace("-","")
        .replace(" ","")
    )


@udf(returnType=StringType())
def clean_names(value: str):
    if "." in value:
        return(value.split(".")[1].strip())
    return(value)


@udf(returnType=StringType())
def get_uf(address: str):
    if "/" in address:
        return(address.split("/")[1].strip())
    return(address)


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
        .format("csv")
        .option("header", "true")
        .load(origin_bucket)
    )


def transform(df):
    phone_cols = ["seller_phone", "buyer_phone", "agent_phone"]
    name_cols = ["seller_name", "buyer_name", "agent_name"]
    address_cols = ["property_address"]

    df_clean = apply_udf_to_columns(df, phone_cols, clean_phones)
    df_clean = apply_udf_to_columns(df_clean, name_cols, clean_names)
    df_clean = apply_udf_to_columns(df_clean, address_cols, get_uf)

    df_clean = df_clean.withColumnRenamed("property_address_applied_udf", "property_state")

    df_clean = df_clean.drop(*(phone_cols + name_cols))

    for c in df_clean.columns:
        if "_clean" in c:
            df_clean = df_clean.withColumnRenamed(c, c.replace("_applied_udf", ""))

    return df_clean


def load(df, destination_bucket: str):
    (
        df
        .write
        .partitionBy("property_state")
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
        .appName("howbootcamp_desafio2")
        .getOrCreate()
    )

    origin_bucket_url = f"s3://raw-440cc93/transactions/"
    destination_bucket_url = f"s3://trusted-a8fe5d9/transactions/"

    print("Starting data extraction...")
    df = extract(spark, origin_bucket_url)
    print("Data extraction done!")
    print("Starting data transformation...")
    df_clean = transform(df)
    print("Data transformation done!")
    print("Started loading data...")
    load(df_clean, destination_bucket_url)
    print("Data load done!")
