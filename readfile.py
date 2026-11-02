"""JSON data reader that returns a Spark DataFrame."""

import argparse
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession


def read_json_file(
    spark: SparkSession,
    input_path: str,
    *,
    multiline: bool = True,
) -> DataFrame:
    """Read JSON records into a DataFrame without managing the SparkSession."""
    return (
        spark.read
        .option("multiLine", str(multiline).lower())
        .json(input_path)
    )


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Read a JSON file into a Spark DataFrame")
    parser.add_argument("--input", required=True, help="JSON data file path")
    parser.add_argument("--show-rows", type=int, default=5, help="Number of rows to preview")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("json-reader").getOrCreate()
    try:
        dataframe = read_json_file(spark, args.input)
        dataframe.printSchema()
        dataframe.show(args.show_rows, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
