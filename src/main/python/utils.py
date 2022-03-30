import glob
import re
import os
import shutil
from pathlib import Path

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

output_dir = "output/"
spark_dir = "output/spark/"
data_dir = "output/analyzed_packages/enriched/enriched_"
analyzed_data_dir = "output/analyzed_data/"
repositories_selected_dir = "output/repositories_selected/"
characterization_dir = "output/characterization/"
visualization_dir = "output/visualization/"
repos_with = "repositories_with_"
api_proportion_file = "api_proportion_"
api_sets_file = "api_sets_"
characterization_file = "characterization_"
visualization_file = "visualization_"

filePath = "filePath"
packageName = "packageName"
className = "className"
methodName = "methodName"
isAPIClass = "isAPIClass"
api = "api"
package = "package"
mcrCategories = "mcrCategories"
mcrTags = "mcrTags"
count = "count"
countAll = "countAll"
proportion = "proportion"
apis = "apis"
dependencies = "dependencies"
packages = "packages"


def read_csv(spark: SparkSession, file_path: str):
    """
    Reads a CSV file with the path `file_path` into a dataframe
    using the spark session `spark`.
    """
    print(f"<--- {file_path}")
    return (
        spark.read.option("inferSchema", "true").option("header", "true").csv(file_path)
    )


def write_csv(df: DataFrame, folder_path: str):
    """
    Writes the dataframe `df` into a folder with the path `folder_path`.
    """
    print(f"---> {df.count()} rows to {folder_path}")
    df.write.format("csv").option("header", "true").save(folder_path)


def copy_csv(input_folder: str, output_file_path: str):
    """
    Copies the CSV file from the folder `input_folder` to the file with the
    file path `output_file_path`.
    """
    print(f"<--> {input_folder} to {output_file_path}")
    Path(os.path.dirname(output_file_path)).mkdir(parents=True, exist_ok=True)
    for file in glob.glob(input_folder + "/*.csv"):
        f = open(file, "r")
        shutil.copy(f.name, output_file_path)
        f.close()


def rename_csv(input_folder: str, output_folder: str, file_prefix: str = ""):
    """
    Copies the partitioned dataframe CSVs file from the folder `input_folder` to
    the file with the folder `output_folder`.
    """
    print(f">--< {input_folder}/**/*.csv to {output_folder}/{file_prefix}*.csv")
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for file in glob.glob(input_folder + "/**/*.csv"):
        newname = re.search(r"=(.*)/", file).group(1)

        os.rename(file, output_folder + "/" + file_prefix + newname + ".csv")


def delete_dir(dir: str):
    """
    Deletes the directory `dir`.
    """
    print(f"---x {dir}")
    path = Path(dir)
    if path.exists() and path.is_dir():
        shutil.rmtree(path)
