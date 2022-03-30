import os
import sys

import utils as u

from typing import List

import seaborn as sns
from pandas import DataFrame as pdDataFrame
import matplotlib.pyplot as plt

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame

PAIRS = [
    ("junit:junit", "org.mockito:mockito-core"),
    ("junit:junit", "org.hamcrest:hamcrest-all"),
    ("junit:junit", "org.openjdk.jmh:jmh-core"),
    # ("junit:junit", "org.openjdk.jmh:jmh-generator-annprocess"),
    ("org.apache.lucene:lucene-core", "org.apache.lucene:lucene-analyzers-common"),
    ("o.springfox:springfox-swagger-ui", "io.springfox:springfox-swagger2"),
    ("org.apache.lucene:lucene-analyzers-common", "org.apache.lucene:lucene-core"),
    ("org.openjdk.jmh:jmh-core", "org.openjdk.jmh:jmh-generator-annprocess"),
    (
        "org.apache.maven.plugin-tools:maven-plugin-annotations",
        "org.apache.maven:maven-plugin-api",
    ),
    ("org.apache.maven:maven-core", "org.apache.maven:maven-plugin-api"),
    ("org.apache.logging.log4j:log4j-api", "org.apache.logging.log4j:log4j-core"),
    (
        "com.fasterxml.jackson.core:jackson-annotations",
        "com.fasterxml.jackson.core:jackson-core",
    ),
    ("org.springframework:spring-beans", "org.springframework:spring-core"),
    ("org.junit.jupiter:junit-jupiter-api", "org.junit.jupiter:junit-jupiter-engine"),
    ("org.apache.curator:curator-framework", "org.apache.curator:curator-recipes"),
    ("org.apache.poi:poi", "org.apache.poi:poi-ooxml"),
    (
        "org.apache.maven.plugin-tools:maven-plugin-annotations",
        "org.apache.maven:maven-core",
    ),
    ("org.eclipse.jetty:jetty-server", "org.eclipse.jetty:jetty-servlet"),
    ("org.springframework:spring-web", "org.springframework:spring-webmvc"),
    ("org.springframework:spring-context", "org.springframework:spring-core"),
]


def read_dir(spark: SparkSession, directory: str) -> DataFrame:
    print(f"Reading {directory}")
    files = [f"{directory}/{file}" for file in os.listdir(directory)]

    return spark.read.option("inferSchema", "true").option("header", "true").csv(files)


def write(df: DataFrame, name: str) -> None:
    print(f"Writing {name}")

    (df.write.format("csv").option("header", "true").mode("overwrite").save(name))


def enrich(df: DataFrame) -> DataFrame:
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
        .drop("one", "packageList", "package")
        .withColumnRenamed("finalPackage", "package")
    )


def analyze(df: DataFrame, group: List, api1: str, api2: str) -> DataFrame:
    fst = F.when(df.api == api1, F.col("package")).otherwise(None)
    snd = F.when(df.api == api2, F.col("package")).otherwise(None)

    return (
        df.filter((df.api == api1) | (df.api == api2))
        .withColumn("packageFirst", fst)
        .withColumn("packageSecond", snd)
        .groupBy(group)
        .agg(
            F.collect_set("packageFirst").alias("pfs"),
            F.collect_set("packageSecond").alias("pss"),
        )
        .filter(F.size(F.col("pfs")) > 0)
        .filter(F.size(F.col("pss")) > 0)
        .select("pfs", "pss")
        .withColumn("first", F.explode("pfs"))
        .withColumn("second", F.explode("pss"))
        .groupBy("first")
        .pivot("second")
        .count()
        .fillna(0)
    )


def visualize(
    res1: pdDataFrame, res2: pdDataFrame, res3: pdDataFrame, api1: str, api2: str
) -> None:
    print(f"Visualizing {api1} and {api2}")
    sns.set(rc={"figure.figsize": (16, 8)})

    f, (ax1, ax2, ax3, axcb) = plt.subplots(
        1, 4, gridspec_kw={"width_ratios": [1, 1, 1, 0.08]}
    )
    ax1.get_shared_y_axes().join(ax2, ax3)
    f.suptitle(f"{api1} and {api2}")

    ax1.set_title(f"Co-occurence per package")
    ax2.set_title(f"Co-occurence per class")
    ax3.set_title(f"Co-occurence per method")

    g1 = sns.heatmap(
        res1,
        annot=True,
        fmt="d",
        annot_kws={"size": 10},
        cmap="YlGnBu",
        xticklabels=1,
        yticklabels=1,
        cbar=False,
        ax=ax1,
    )
    g1.set_ylabel("")
    g2 = sns.heatmap(
        res2,
        annot=True,
        fmt="d",
        annot_kws={"size": 10},
        cmap="YlGnBu",
        xticklabels=1,
        yticklabels=0,
        ax=ax2,
        cbar=False,
    )
    g2.set_ylabel("")
    g3 = sns.heatmap(
        res3,
        annot=True,
        fmt="d",
        annot_kws={"size": 10},
        cmap="YlGnBu",
        xticklabels=1,
        yticklabels=0,
        ax=ax3,
        cbar_ax=axcb,
    )
    g3.set_ylabel("")

    for ax in [g1, g2, g3]:
        tl = ax.get_xticklabels()
        ax.set_xticklabels(tl, rotation=50, ha="right")
        tly = ax.get_yticklabels()
        ax.set_yticklabels(tly, rotation=0)

    plt.subplots_adjust(left=0.25, right=0.9, bottom=0.3, top=0.85)

    print("Saving figure...")
    plt.savefig(f"output/analyzed_packages/{api1}_{api2}.png", dpi=500)
    # plt.show()


def create_repositories_file(df: DataFrame)->None:
    print("Creating repositories file...")

    u.delete_dir(u.spark_dir)
    u.write_csv(
        df.groupBy("repository")
        .agg(F.collect_set("package").alias("packageList"))
        .withColumn("pre", F.lit("["))
        .withColumn("post", F.lit("]"))
        .withColumn(
            "packages", F.concat("pre", F.array_join("packageList", ","), "post")
        )
        .withColumn("repositoryName", F.regexp_replace("repository", "_", "/"))
        .select("repositoryName", "packages"),
        u.spark_dir,
    )
    u.copy_csv(u.spark_dir, "./output/repositories_with_packages.csv")
    u.delete_dir(u.spark_dir)

def create_dependencies_with_packages_file(df: DataFrame)->None:
    print("Creating dependencies with packages file...")

    u.delete_dir(u.spark_dir)
    u.write_csv(
        df.groupBy("package")
        .agg(F.first("api").alias("api")),
        u.spark_dir,
    )
    u.copy_csv(u.spark_dir, "./output/dependencies_with_packages.csv")
    u.delete_dir(u.spark_dir)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("MSR")
        .config("spark.driver.memory", "10g")
        .config("spark.executor.memory", "15g")
        .config("spark.executor.cores", "4")
        .getOrCreate()
    )

    if os.path.exists("./output/analyzed_packages/enriched"):
        print("Joined table already exists")
        df = read_dir(spark, "./output/analyzed_packages/enriched")
    else:
        print("Joined table does not exist yet, running full analysis")
        df = read_dir(spark, "./output/data")
        df = enrich(df)

        write(df, "./output/analyzed_packages/enriched")
        df = read_dir(spark, "./output/analyzed_packages/enriched")

    create_repositories_file(df)
    create_dependencies_with_packages_file(df)

    for api1, api2 in PAIRS:
        try:
            groupPkg = ["repository", "packageName"]
            groupCls = ["repository", "packageName", "className"]
            groupMtd = ["repository", "packageName", "className", "methodName"]

            res1 = analyze(df, groupPkg, api1, api2).toPandas().set_index("first")
            res2 = analyze(df, groupCls, api1, api2).toPandas().set_index("first")
            res3 = analyze(df, groupMtd, api1, api2).toPandas().set_index("first")

            res1, res2 = res1.align(res2, fill_value=0)
            res2, res3 = res2.align(res3, fill_value=0)
            res1, res3 = res1.align(res3, fill_value=0)

            visualize(res1, res2, res3, api1, api2)
        except Exception as e:
            print(f"Failed to visualize {api1} and {api2}")
            print(e)
