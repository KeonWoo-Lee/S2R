"""Run the JSON read, schema normalization, and flattening steps together."""

import argparse
from typing import Sequence

from pyspark.sql import SparkSession

if __package__:
    from .flatten import flatten
    from .readfile import read_json_file
    from .schema_reader import read_schema_tree
else:
    from flatten import flatten
    from readfile import read_json_file
    from schema_reader import read_schema_tree


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Read JSON data and schema, then create relational DataFrames"
    )
    parser.add_argument("--data", required=True, help="JSON records file path")
    parser.add_argument("--schema", required=True, help="JSON Schema file path")
    parser.add_argument(
        "--record-key-path",
        help="Optional dot-separated source field path to use as the root record id",
    )
    parser.add_argument("--show-rows", type=int, default=3, help="Rows to preview per table")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("semi-structured-flatten").getOrCreate()
    try:
        dataframe = read_json_file(spark, args.data)
        schema_tree = read_schema_tree(args.schema)
        tables = flatten(
            dataframe,
            schema_tree,
            record_key_path=args.record_key_path,
        )

        for table_name, table_dataframe in tables.items():
            print(f"\nTable: {table_name}")
            print(f"Rows: {table_dataframe.count()}")
            table_dataframe.printSchema()
            table_dataframe.show(args.show_rows, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
