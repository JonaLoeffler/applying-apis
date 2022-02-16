import os

import pyspark.sql.functions as F
from pyspark import SparkContext
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, MinHashLSH
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType


def read(file: str):
    return (
        spark.read.option("inferSchema", "true")
        .option("header", "true")
        .csv(f"./output/data/{file}")
    )


def write(df, name):
    df.toPandas().to_csv(name, sep=",", header=True, index=False)


def aggregate(df):
    return (
        df.filter(df.isAPIClass == "true")
        .groupby(["packageName"])
        .agg(F.count("mcrCategories").alias("count"))
    )


if __name__ == "__main__":
    sc = SparkContext("local", "msr3")
    spark = SparkSession(sc)

    files = os.listdir("./output/data")
    os.mkdir("./output/analyzed_packages")

    for file in files:
        df = read(file)
        df = aggregate(df)
        write(df, "./output/analyzed_packages/" + file.replace("data_", "packages_"))
