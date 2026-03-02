import functools
import logging

import pandas as pd
import pyarrow
from pyiceberg.expressions import And, BooleanExpression, EqualTo
from pyiceberg.table import StaticTable

import process_report.invoices.invoice as invoice
from process_report.data_tools.config import data_tools_settings

logger = logging.getLogger(__name__)
FilterValue = str | int | float

_LIFETIME_COLS = [
    invoice.PROJECT_ID_FIELD,
    invoice.CLUSTER_NAME_FIELD,
    invoice.COST_FIELD,
]


def _row_filter(**filters: FilterValue) -> BooleanExpression | None:
    """Combine column equality checks into a single PyIceberg filter expression.

    Each keyword argument becomes one equality check (column == value).
    Multiple checks are joined with AND.

    Args:
        **filters: Column names as keys, values to filter by. Values must be str, int, or float.

    Returns:
        A PyIceberg BooleanExpression combining all checks, or None if no filters were given.
    """
    if not filters:
        return None
    clauses = [EqualTo(col, val) for col, val in filters.items()]
    return functools.reduce(And, clauses)


@functools.cache
def get_table() -> StaticTable:
    return StaticTable.from_metadata(
        data_tools_settings.table_path,
        properties=data_tools_settings.iceberg_s3_properties(),
    )


@functools.cache
def get_invoice_dataframe(
    cols: tuple[str, ...] | None = None, **filters: FilterValue
) -> pd.DataFrame:
    """Load invoice data from the Iceberg table.

    Args:
        cols: Column names to select as a tuple. None selects all columns.
        **filters: Column names as keys, values to filter by. Values must be str, int, or float.

    Returns:
        DataFrame of invoice data from the table.
    """
    table = get_table()
    scan = table.scan(row_filter=_row_filter(**filters))
    if cols:
        scan = scan.select(*cols)
    df = scan.to_pandas()
    if filters and df.empty:
        logger.warning("No invoice rows matched filters: %s", filters)
    return df


def group_and_sum(
    df: pd.DataFrame,
    group_by: tuple[str, ...],
    *,
    agg_col: str,
    agg_name: str = "total",
) -> pd.DataFrame:
    """Group a dataframe and aggregate one column with sum.

    Args:
        df: Input dataframe.
        group_by: Column names to group by.
        agg_col: Column to sum.
        agg_name: Name for the aggregated column in the output. Defaults to "total".

    Returns:
        DataFrame with one row per group and a column containing the sum of agg_col.

    Raises:
        ValueError: If agg_col is not present in df.
        TypeError: If agg_col is not a numeric column.
    """
    # for the empty case — there is nothing to aggregate, so return an empty frame with the correct output columns immediately for correct dtype
    if df.empty:
        return pd.DataFrame(columns=[*group_by, agg_name])

    if agg_col not in df.columns:
        raise ValueError(
            f"Aggregation column '{agg_col}' not found in dataframe. "
            f"Available columns: {list(df.columns)}"
        )

    dtype = df[agg_col].dtype
    is_numeric = pd.api.types.is_numeric_dtype(dtype) or (
        isinstance(dtype, pd.ArrowDtype)
        and (
            pyarrow.types.is_integer(dtype.pyarrow_dtype)
            or pyarrow.types.is_floating(dtype.pyarrow_dtype)
            or pyarrow.types.is_decimal(dtype.pyarrow_dtype)
        )
    )
    if not is_numeric:
        raise TypeError(
            f"Aggregation column '{agg_col}' must be numeric but has dtype {dtype!r}"
        )

    decimal_dtype = pd.ArrowDtype(pyarrow.decimal128(21, 2))
    grouped_input = df.copy()
    grouped_input[agg_col] = grouped_input[agg_col].fillna(0).astype(decimal_dtype)
    agg_spec = {agg_name: (agg_col, "sum")}
    return grouped_input.groupby(list(group_by), as_index=False).agg(**agg_spec)


def aggregate_by(
    cols: tuple[str, ...],
    group_by: tuple[str, ...],
    *,
    agg_col: str,
    agg_name: str = "total",
    **filters: FilterValue,
) -> pd.DataFrame:
    """Load invoice data and return grouped sum totals.

    This helper fetches invoice rows using the provided selected columns and filters,
    ensures grouping columns are included in the selection, then returns a grouped sum
    aggregation over ``agg_col``.

    Args:
        cols: Columns to select from the invoice table before aggregation.
        group_by: Columns to group rows by in the aggregated output.
        agg_col: Numeric column to sum within each group.
        agg_name: Output column name for the aggregated sum. Defaults to ``"total"``.
        **filters: Column=value equality filters applied while loading invoice data.
            Values must be str, int, or float.

    Returns:
        DataFrame with one row per unique ``group_by`` combination and a summed
        ``agg_name`` column quantized to two decimal places.

    Example:
        >>> df = aggregate_by(
        ...     cols=(invoice.COST_FIELD,),
        ...     group_by=(invoice.PROJECT_ID_FIELD, invoice.CLUSTER_NAME_FIELD),
        ...     agg_col=invoice.COST_FIELD,
        ...     agg_name="lifetime_allocation_cost",
        ... )
    """
    all_cols = list(cols)
    for col in group_by:
        if col not in all_cols:
            all_cols.append(col)
    df = get_invoice_dataframe(tuple(all_cols), **filters)
    return group_and_sum(
        df,
        group_by=group_by,
        agg_col=agg_col,
        agg_name=agg_name,
    )


def calculate_lifetime_costs(**filters: FilterValue) -> pd.DataFrame:
    """Group invoice data by project and cluster, summing the COST column per group.

    Args:
        **filters: Column names as keys, values to filter by. Values must be str, int, or float.

    Returns:
        DataFrame with columns: Project - Allocation, Cluster Name, lifetime_allocation_cost.

    Example:
        >>> filters = {invoice.PROJECT_ID_FIELD: "vllm-test"}
        >>> df = calculate_lifetime_costs(**filters)
    """

    return aggregate_by(
        tuple(_LIFETIME_COLS),
        (invoice.PROJECT_ID_FIELD, invoice.CLUSTER_NAME_FIELD),
        agg_col=invoice.COST_FIELD,
        agg_name="lifetime_allocation_cost",
        **filters,
    )
