import functools
from decimal import Decimal
import logging

import pandas as pd
from pyiceberg.expressions import And, BooleanExpression, EqualTo

import process_report.invoices.invoice as invoice
from process_report.data_tools.config import get_table

logger = logging.getLogger(__name__)
FilterValue = str | int | float

_LIFETIME_COLS = [
    invoice.PROJECT_ID_FIELD,
    invoice.CLUSTER_NAME_FIELD,
    invoice.BALANCE_FIELD,
]


def _row_filter(**filters: FilterValue) -> BooleanExpression | None:
    """Build a PyIceberg row filter expression from column=value filters.

    Args:
        **filters: Column names as keys, values to filter by. Values must be str, int, or float.

    Returns:
        PyIceberg BooleanExpression like EqualTo(col1, 'x') AND EqualTo(col2, 1),
        or None if no filters are given.
    """
    if not filters:
        return None
    expression: BooleanExpression | None = None
    for col, val in filters.items():
        clause = EqualTo(col, val)
        expression = clause if expression is None else And(expression, clause)
    return expression


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
    row_filter = _row_filter(**filters)
    if row_filter:
        scan = table.scan(row_filter=row_filter)
    else:
        scan = table.scan()
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
    """
    grouped_input = df.copy()
    grouped_input[agg_col] = grouped_input[agg_col].fillna(0)
    agg_spec = {agg_name: (agg_col, "sum")}
    grouped_df = grouped_input.groupby(list(group_by), as_index=False).agg(**agg_spec)
    grouped_df[agg_name] = grouped_df[agg_name].map(
        lambda v: Decimal(str(v)).quantize(Decimal("0.01"))
    )
    return grouped_df


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
        ...     cols=(invoice.BALANCE_FIELD,),
        ...     group_by=(invoice.PROJECT_ID_FIELD, invoice.CLUSTER_NAME_FIELD),
        ...     agg_col=invoice.BALANCE_FIELD,
        ...     agg_name="lifetime_allocation_balance",
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
    """Group invoice data by project and cluster, summing balance per group.

    Args:
        **filters: Column names as keys, values to filter by. Values must be str, int, or float.

    Returns:
        DataFrame with columns: Project - Allocation, Cluster Name, lifetime_allocation_balance.

    Example:
        >>> filters = {invoice.PROJECT_ID_FIELD: "vllm-test"}
        >>> df = calculate_lifetime_costs(**filters)
    """

    return aggregate_by(
        tuple(_LIFETIME_COLS),
        (invoice.PROJECT_ID_FIELD, invoice.CLUSTER_NAME_FIELD),
        agg_col=invoice.BALANCE_FIELD,
        agg_name="lifetime_allocation_balance",
        **filters,
    )
