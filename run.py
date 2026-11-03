"""Run the JSON read, schema normalization, and flattening steps together."""

import argparse
import os
from typing import Sequence

from pyspark.sql import SparkSession

if __package__:
    from .flatten import flatten
    from .readfile import read_json_file
    from .schema_reader import read_schema_tree
    from .write import write_dataframes_to_jdbc
else:
    from flatten import flatten
    from readfile import read_json_file
    from schema_reader import read_schema_tree
    from write import write_dataframes_to_jdbc


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Read JSON data and schema, then create relational DataFrames"
    )
    parser.add_argument("--data", required=True, help="JSON records file path")
    parser.add_argument("--schema", required=True, help="JSON Schema file path")
    parser.add_argument(
        "--write-to-postgres",
        action="store_true",
        help="Write flattened DataFrames to PostgreSQL via JDBC",
    )
    parser.add_argument(
        "--write-mode",
        choices=("errorifexists", "append", "overwrite", "ignore"),
        default="errorifexists",
        help="JDBC save mode (default: errorifexists)",
    )
    parser.add_argument(
        "--table-prefix",
        default=os.getenv("S2R_PG_TABLE_PREFIX", "s2r"),
        help="Prefix for destination tables (default: s2r)",
    )
    parser.add_argument(
        "--postgres-driver-jar",
        default=os.getenv("S2R_POSTGRES_JDBC_JAR"),
        help="Optional local path to the PostgreSQL JDBC driver JAR",
    )
    parser.add_argument(
        "--record-key-path",
        help="Optional dot-separated source field path to use as the root record id",
    )
    parser.add_argument("--show-rows", type=int, default=3, help="Rows to preview per table")
    args = parser.parse_args(argv)

    spark_builder = SparkSession.builder.appName("semi-structured-flatten")
    if args.write_to_postgres and args.postgres_driver_jar:
        spark_builder = spark_builder.config("spark.jars", args.postgres_driver_jar)
    spark = spark_builder.getOrCreate()
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

        if args.write_to_postgres:
            jdbc_url = os.getenv("S2R_PG_URL")
            username = os.getenv("S2R_PG_USER")
            password = os.getenv("S2R_PG_PASSWORD")
            missing = [
                name
                for name, value in (
                    ("S2R_PG_URL", jdbc_url),
                    ("S2R_PG_USER", username),
                    ("S2R_PG_PASSWORD", password),
                )
                if not value
            ]
            if missing:
                raise ValueError(
                    "PostgreSQL output requires environment variables: "
                    + ", ".join(missing)
                )

            written_tables = write_dataframes_to_jdbc(
                tables,
                jdbc_url=jdbc_url,
                username=username,
                password=password,
                table_prefix=args.table_prefix,
                mode=args.write_mode,
            )
            print("\nPostgreSQL tables written:")
            for source_name, destination_name in written_tables.items():
                print(f"  {source_name} -> {destination_name}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
