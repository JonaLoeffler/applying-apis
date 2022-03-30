import pyspark.sql.functions as F
from pyspark import SparkContext
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, MinHashLSH
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType

import utils as u


def countEach(column: str, new_column: str):
    """
    Counts each object in the column `column` ("dependencies" or "mcrTags") of the file in `df`
    and saves the results in a new file with the column `new_column`.
    """
    u.delete_dir(u.spark_dir)

    df = u.read_csv(spark, u.output_dir + u.repos_with + column + ".csv")
    df = df.withColumn(column, F.split(
        F.regexp_replace(column, "[\[\]]", ""), ","))

    list = []
    for row in df.rdd.collect():
        for i in row[column]:
            list.append(i)

    df1 = u.read_csv(spark, u.output_dir + "dependencies_with_packages.csv")

    data = spark.createDataFrame(list, StringType()).toDF(column) \
        .groupBy(column).count().withColumnRenamed(column, new_column) \

    data = data.join(df1, df1.package == data.package).drop(df1.package)

    u.write_csv(data.coalesce(1), u.spark_dir)
    u.copy_csv(u.spark_dir, u.output_dir + column + "_counted.csv")


def countSets(column: str):
    """
    Counts each set in the column `column` ("dependencies" or "mcrTags") of the file in `df`
    and saves the results in a new file.
    """
    u.delete_dir(u.spark_dir)

    df = u.read_csv(spark, u.output_dir + u.repos_with + column + ".csv")
    df = df.groupBy(column).count()

    u.write_csv(df.coalesce(1), u.spark_dir)
    u.copy_csv(u.spark_dir, u.output_dir + column + "_sets_counted.csv")


def computeJaccardSimilarity(column: str, threshold: float):
    """
    Computes the Jaccard similarity with `threshold` on the column `column` ("dependencies" or "mcrTags")
    of the file in `df` and saves the results in a new file.
    """
    u.delete_dir(u.spark_dir)

    df = u.read_csv(spark, u.output_dir + u.repos_with + column + ".csv")
    df = df.withColumn(column, F.split(
        F.regexp_replace(column, "[\[\]]", ""), ","))

    model = Pipeline(stages=[
        HashingTF(inputCol=column, outputCol="vectors"),
        MinHashLSH(inputCol="vectors", outputCol="lsh", numHashTables=10)
    ]).fit(df)

    data_t = model.transform(df)
    data_s = model.stages[-1].approxSimilarityJoin(
        data_t, data_t, 1 - threshold, distCol="similarity")

    result = data_s.withColumn("intersection", F.array_intersect(
        F.col("datasetA." + column), F.col("datasetB." + column))) \
        .select(F.col("datasetA.repositoryName").alias("repositoryName1"),
                F.col("datasetB.repositoryName").alias("repositoryName2"),
                F.col("intersection"), F.col("similarity")) \
        .filter("repositoryName1 < repositoryName2") \
        .withColumn("similarity", F.round(1 - F.col("similarity"), 2)) \
        .withColumn('intersectionSize', F.size(F.col('intersection'))) \
        .filter(F.size(F.col("intersection")) >= 5) \
        .orderBy('similarity', ascending=False) \
        .select('repositoryName1', 'repositoryName2', 'intersectionSize', 'similarity')
    u.write_csv(result.coalesce(1), u.spark_dir)
    u.copy_csv(u.spark_dir, u.output_dir +
               u.repos_with + column + "_similarity.csv")


def countPairs():
    """
    Creates all package pairs with dependencies (count >= 100) in the file in `df2`,
    counts the occurrences of each package pair in the file in `df1` and saves the
    results in a new file.
    """
    u.delete_dir(u.spark_dir)

    df1 = u.read_csv(spark, u.output_dir + u.repos_with +
                     u.packages + ".csv")
    df1 = df1.withColumn(u.packages, F.split(
        F.regexp_replace(u.packages, "[\[\]]", ""), ","))

    df2 = u.read_csv(spark, u.output_dir + u.packages + "_counted.csv")
    df2 = df2.filter(F.col("count") >= 100)

    pairs = df2.select(F.col("package").alias("package1")) \
        .crossJoin(df2.select(F.col("package").alias("package2"))) \
        .filter("package1 < package2")

    counted = pairs.join(df1, F.array_contains(df1[u.packages], pairs["package1"]) &
                         F.array_contains(df1[u.packages], pairs["package2"])) \
        .groupBy("package1", "package2").count().drop("repositoryName").drop(u.packages)

    df3 = df2.withColumnRenamed("package", "package1") \
        .withColumnRenamed("count", "count1") \
        .withColumnRenamed("api", "api1")
    df4 = df2.withColumnRenamed("package", "package2") \
        .withColumnRenamed("count", "count2") \
        .withColumnRenamed("api", "api2")

    data = counted.join(df3, "package1").join(df4, "package2") \
        .select("api1", "package1", "api2", "package2", "count", "count1", "count2") \
        .filter("api1 != api2")
    data = data.withColumn("proportion1", F.round(data["count"] / data["count1"], 2)) \
        .withColumn("proportion2",  F.round(data["count"] / data["count2"], 2)) \
        .withColumn("maxProportion", F.greatest(F.col("proportion1"), F.col("proportion2"))) \
        .orderBy('maxProportion', ascending=False)

    u.write_csv(data.coalesce(1), u.spark_dir)
    u.copy_csv(u.spark_dir, u.output_dir +
               u.packages + "_pairs_counted.csv")


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("MSR")
        .config("spark.driver.memory", "10g")
        .config("spark.executor.memory", "15g")
        .config("spark.executor.cores", "4")
        .getOrCreate()
    )

    countEach("packages", "package")
    countSets("packages")
    #countSets("mcrTags")
    computeJaccardSimilarity("packages", 0.7)
    #computeJaccardSimilarity("mcrTags", 0.7)
    countPairs()
    print("Done!")
