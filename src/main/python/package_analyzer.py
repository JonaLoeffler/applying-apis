import os

import pyspark.sql.functions as F
from pyspark import SparkContext
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, MinHashLSH
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType


def read():
    files = [f"./output/data/{file}" for file in os.listdir("./output/data")]

    return spark.read.option("inferSchema", "true").option("header", "true").csv(files)


def write(df, name):
    (df.write.format("csv").option("header", "true").mode("overwrite").save(name))


if __name__ == "__main__":
    sc = SparkContext("local", "msr3")
    spark = SparkSession(sc)

    df = read()
    df = df.filter(df.isAPIClass == "true").withColumn(
        "usedPackageOfElement",
        F.regexp_extract("usedClassOfElement", "(.*)\\.[A-Z]", 1),
    )

    agg = (
        df.groupby(["usedPackageOfElement"])
        .agg(F.count("mcrCategories").alias("count"))
        .orderBy("count")
    )

    write(agg, "./output/analyzed_packages/aggregated")
