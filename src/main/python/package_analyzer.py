import os

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark import SparkContext
from pyspark.sql.session import SparkSession



def read_dir(spark: SparkSession, directory: str):
    print(f"Reading {directory}")
    files = [f"{directory}/{file}" for file in os.listdir(directory)]

    return spark.read.option("inferSchema", "true").option("header", "true").csv(files)


def write(df, name):
    print(f"Writing {name}")

    (df.write.format("csv").option("header", "true").mode("overwrite").save(name))


def enrich(df):
    # basic filtering & creating columns
    df = (
        df.filter(df.isAPIClass == "true")
        .withColumn("filename", F.input_file_name())
        .withColumn("repository", F.regexp_extract("filename", "data_(.*)\\.csv", 1))
        .withColumn(
            "package",
            F.regexp_extract("usedClassOfElement", "^(.*?)\\.[A-Z]", 1),
        )
        .withColumn("packageList", F.split("package", "\\."))
        .withColumn("packageDepth", F.size("packageList"))
    )

    # determine the actual api base packages
    apiWindow = Window.partitionBy("api").orderBy(F.col("packageDepth").asc())
    basePackages = (
        df.withColumn("row", F.row_number().over(apiWindow))
        .filter(F.col("row") == 1)
        .drop("row")
        .select("api", "package")
        .distinct()
        .withColumn("basePackage", F.col("package"))
        .withColumn("basePackageDepth", F.size(F.split("basePackage", "\\.")))
        .drop("package")
    )

    # join and find final package list which is base plus one level
    return (
        df.join(basePackages, df.api == basePackages.api)
        .drop(basePackages.api)
        .withColumn("one", F.lit(1))
        .withColumn(
            "finalPackage",
            F.array_join(
                F.slice(
                    "packageList",
                    F.col("one"),
                    F.col("basePackageDepth") + F.col("one"),
                ),
                ".",
            ),
        )
        .drop("one", "packageList")
    )


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("MSR")
        .config("spark.driver.memory", "10g")
        .config("spark.executor.memory", "15g")
        .config("spark.executor.cores", "4")
        .getOrCreate()
    )

    if os.path.exists("./output/analyzed_packages/joined"):
        print("Joined table already exists")
        df = read_dir(spark, "./output/analyzed_packages/joined")
    else:
        print("Joined table does not exist yet, running full analysis")
        df = read_dir(spark, "./output/data")
        df = enrich(df)

        write(df, "./output/analyzed_packages/joined")

    agg = (
        df.groupby("api", "finalPackage")
        .agg(F.count("api").alias("count"))
        .orderBy("count")
    )

    write(agg, "./output/analyzed_packages/aggregated")
