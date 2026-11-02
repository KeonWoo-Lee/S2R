"""Flatten a Spark DataFrame according to a normalized schema tree.

Objects contribute scalar columns to their current table. Each Array creates a
child table with the parent row id and the source array position.
"""

from typing import Any

from pyspark.sql import Column, DataFrame, functions as F


def _column(path: tuple[str, ...]) -> Column:
    """Build a Spark column reference while safely quoting nested field names."""
    if not path:
        raise ValueError("A column path cannot be empty")

    quoted_path = ".".join(f"`{part.replace('`', '``')}`" for part in path)
    return F.col(quoted_path)


def _path_name(path: tuple[str, ...], separator: str) -> str:
    return separator.join(path)


def _select_unique_columns(
    dataframe: DataFrame,
    columns: list[tuple[str, Column]],
    *,
    table_name: str,
) -> DataFrame:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for name, _ in columns:
        if name in seen:
            duplicates.add(name)
        seen.add(name)
    if duplicates:
        raise ValueError(
            f"Column paths collide in table {table_name}: {', '.join(sorted(duplicates))}"
        )
    return dataframe.select(*(column for _, column in columns))


def _collect_columns(
    node: dict[str, Any],
    dataframe: DataFrame,
    source_path: tuple[str, ...],
    output_path: tuple[str, ...],
    child_tables: dict[str, DataFrame],
    *,
    record_id_column: str,
    parent_id_column: str,
    array_index_column: str,
    column_separator: str,
) -> list[tuple[str, Column]]:
    kind = node.get("kind")

    if kind == "scalar":
        scalar_path = output_path or ("value",)
        name = _path_name(scalar_path, column_separator)
        return [(name, _column(source_path).alias(name))]

    if kind == "object":
        expressions: list[tuple[str, Column]] = []
        for field in node.get("fields", []):
            field_name = field["name"]
            field_node = field["node"]
            field_source_path = source_path + (field_name,)
            field_output_path = output_path + (field_name,)

            if field_node.get("kind") == "array":
                _build_array_table(
                    field_node,
                    dataframe,
                    field_source_path,
                    field_output_path,
                    child_tables,
                    record_id_column=record_id_column,
                    parent_id_column=parent_id_column,
                    array_index_column=array_index_column,
                    column_separator=column_separator,
                )
            else:
                expressions.extend(
                    _collect_columns(
                        field_node,
                        dataframe,
                        field_source_path,
                        field_output_path,
                        child_tables,
                        record_id_column=record_id_column,
                        parent_id_column=parent_id_column,
                        array_index_column=array_index_column,
                        column_separator=column_separator,
                    )
                )
        return expressions

    if kind == "array":
        _build_array_table(
            node,
            dataframe,
            source_path,
            output_path,
            child_tables,
            record_id_column=record_id_column,
            parent_id_column=parent_id_column,
            array_index_column=array_index_column,
            column_separator=column_separator,
        )
        return []

    raise ValueError(f"Unsupported schema node kind: {kind}")


def _build_array_table(
    array_node: dict[str, Any],
    parent_dataframe: DataFrame,
    source_path: tuple[str, ...],
    table_path: tuple[str, ...],
    child_tables: dict[str, DataFrame],
    *,
    record_id_column: str,
    parent_id_column: str,
    array_index_column: str,
    column_separator: str,
) -> None:
    table_name = _path_name(table_path, column_separator)
    if table_name in child_tables:
        raise ValueError(f"Two Array paths produce the same table name: {table_name}")

    element_column = "__etl_array_element"
    child_dataframe = (
        parent_dataframe.select(
            _column((record_id_column,)).alias(parent_id_column),
            F.posexplode(_column(source_path)).alias(array_index_column, element_column),
        )
        .withColumn(record_id_column, F.monotonically_increasing_id())
    )

    item_node = array_node.get("items")
    if item_node is None:
        raise ValueError(f"Array schema has no item node: {table_name}")

    item_kind = item_node.get("kind")
    if item_kind == "scalar":
        item_output_path = table_path + ("value",)
    elif item_kind == "array":
        item_output_path = table_path + ("items",)
    else:
        item_output_path = table_path

    value_columns = _collect_columns(
        item_node,
        child_dataframe,
        (element_column,),
        item_output_path,
        child_tables,
        record_id_column=record_id_column,
        parent_id_column=parent_id_column,
        array_index_column=array_index_column,
        column_separator=column_separator,
    )

    columns = [
        (record_id_column, _column((record_id_column,)).alias(record_id_column)),
        (parent_id_column, _column((parent_id_column,)).alias(parent_id_column)),
        (array_index_column, _column((array_index_column,)).alias(array_index_column)),
    ]
    child_tables[table_name] = _select_unique_columns(
        child_dataframe,
        columns + value_columns,
        table_name=table_name,
    )


def flatten(
    dataframe: DataFrame,
    schema_tree: dict[str, Any],
    *,
    root_table: str = "root",
    record_id_column: str = "__etl_record_id",
    parent_id_column: str = "__etl_parent_record_id",
    array_index_column: str = "__etl_array_index",
    column_separator: str = ".",
    record_key_path: str | None = None,
) -> dict[str, DataFrame]:
    """Return a DataFrame per table path, including the root table.

    If ``record_key_path`` is supplied, it is a dot-separated source field path
    used as the root id. Otherwise a Spark-generated id is used; generated ids
    are unique within this execution and are not stable across separate runs.
    """
    if schema_tree.get("kind") != "object":
        raise ValueError("The root schema node must be an Object")
    if record_id_column in dataframe.columns:
        raise ValueError(f"Reserved id column already exists in input: {record_id_column}")

    if record_key_path:
        key_parts = tuple(record_key_path.split("."))
        root_dataframe = dataframe.withColumn(
            record_id_column,
            _column(key_parts),
        )
    else:
        root_dataframe = dataframe.withColumn(
            record_id_column,
            F.monotonically_increasing_id(),
        )

    child_tables: dict[str, DataFrame] = {}
    value_columns = _collect_columns(
        schema_tree,
        root_dataframe,
        (),
        (),
        child_tables,
        record_id_column=record_id_column,
        parent_id_column=parent_id_column,
        array_index_column=array_index_column,
        column_separator=column_separator,
    )

    root_dataframe = _select_unique_columns(
        root_dataframe,
        [
            (record_id_column, _column((record_id_column,)).alias(record_id_column)),
            *value_columns,
        ],
        table_name=root_table,
    )

    if root_table in child_tables:
        raise ValueError(f"Root table name collides with an Array table: {root_table}")

    # Return parent Array tables before their nested descendants.
    ordered_children = dict(
        sorted(
            child_tables.items(),
            key=lambda item: (item[0].count(column_separator), item[0]),
        )
    )
    return {root_table: root_dataframe, **ordered_children}
