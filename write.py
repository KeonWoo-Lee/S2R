"""Write flattened Spark DataFrames to a JDBC database."""

import re

from pyspark.sql import DataFrame


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _table_name(prefix: str, dataframe_name: str) -> str:
    """Map a flattened table path to a safe PostgreSQL table identifier."""
    suffix = dataframe_name.replace(".", "_")
    name = f"{prefix}_{suffix}" if prefix else suffix
    if not _IDENTIFIER.fullmatch(name):
        raise ValueError(
            f"Invalid SQL table name {name!r}; use letters, digits, and underscores "
            "and start with a letter or underscore"
        )
    return name


def write_dataframes_to_jdbc(
    dataframes: dict[str, DataFrame],
    *,
    jdbc_url: str,
    username: str,
    password: str,
    table_prefix: str = "s2r",
    mode: str = "errorifexists",
    driver: str = "org.postgresql.Driver",
    batch_size: int = 1000,
) -> dict[str, str]:
    """Write each flattened DataFrame to a table, parents before child tables.

    ``mode`` follows Spark's JDBC save modes. The default fails when a destination
    table already exists, so reruns cannot silently duplicate or replace data.
    """
    if mode not in {"append", "overwrite", "ignore", "error", "errorifexists"}:
        raise ValueError(f"Unsupported JDBC save mode: {mode}")
    if batch_size < 1:
        raise ValueError("batch_size must be a positive integer")

    written_tables: dict[str, str] = {}
    for dataframe_name, dataframe in dataframes.items():
        destination = _table_name(table_prefix, dataframe_name)
        (
            dataframe.write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", destination)
            .option("user", username)
            .option("password", password)
            .option("driver", driver)
            .option("batchsize", str(batch_size))
            .mode(mode)
            .save()
        )
        written_tables[dataframe_name] = destination

    return written_tables
