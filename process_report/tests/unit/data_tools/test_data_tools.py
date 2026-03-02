from decimal import Decimal
from unittest import mock

import pandas as pd
import pyarrow
import pytest

from process_report.data_tools import costs

# These are the column names in the iceberg table using string literals instead of the invoice module to test column name correctness
PID = "Project - Allocation ID"
CLUSTER = "Cluster Name"
COST = "Cost"


@pytest.fixture(autouse=True)
def clear_dataframe_cache():
    costs.get_invoice_dataframe.cache_clear()
    yield
    costs.get_invoice_dataframe.cache_clear()


@pytest.fixture
def sample_invoice_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            PID: ["vllm-test", "vllm-test", "webrca-1b021a"],
            CLUSTER: ["ocp-test", "ocp-test", "ocp-prod"],
            COST: [1.234, 2.345, None],
        }
    )


def test_row_filter_empty_returns_none():
    assert costs._row_filter() is None


@pytest.mark.parametrize(
    "filters",
    [
        {PID: "vllm-test", CLUSTER: "ocp-test"},
        {PID: "vllm-test", CLUSTER: "ocp-prod"},
    ],
)
def test_row_filter_builds_combined_and_expression(filters: dict[str, str]):
    expression = costs._row_filter(**filters)
    assert isinstance(expression, costs.And)
    assert isinstance(expression.left, costs.EqualTo)
    assert isinstance(expression.right, costs.EqualTo)


def test_aggregate_by_rounds_and_forwards_filters(
    monkeypatch: pytest.MonkeyPatch, sample_invoice_dataframe: pd.DataFrame
):
    mock_loader = mock.MagicMock(return_value=sample_invoice_dataframe)
    monkeypatch.setattr(costs, "get_invoice_dataframe", mock_loader)

    result = costs.aggregate_by(
        (COST,),
        (PID, CLUSTER),
        agg_col=COST,
        agg_name="lifetime_allocation_cost",
        **{PID: "vllm-test"},
    )

    args, kwargs = mock_loader.call_args
    assert args == ((COST, PID, CLUSTER),)
    assert kwargs == {PID: "vllm-test"}

    decimal_dtype = pd.ArrowDtype(pyarrow.decimal128(21, 2))
    expected = pd.DataFrame(
        {
            PID: ["vllm-test", "webrca-1b021a"],
            CLUSTER: ["ocp-test", "ocp-prod"],
            "lifetime_allocation_cost": pd.array(
                [Decimal("3.58"), Decimal("0.00")], dtype=decimal_dtype
            ),
        }
    )
    assert result.equals(expected)


def test_group_and_sum_raises_on_missing_column(sample_invoice_dataframe: pd.DataFrame):
    with pytest.raises(ValueError, match="not found in dataframe"):
        costs.group_and_sum(
            sample_invoice_dataframe,
            (PID, CLUSTER),
            agg_col="non_existent_column",
            agg_name="lifetime_allocation_cost",
        )


def test_group_and_sum_raises_on_non_numeric_column(
    sample_invoice_dataframe: pd.DataFrame,
):
    with pytest.raises(TypeError, match="must be numeric"):
        costs.group_and_sum(
            sample_invoice_dataframe,
            (CLUSTER,),
            agg_col=PID,
            agg_name="lifetime_allocation_cost",
        )


def test_group_and_sum_is_pure_transform(sample_invoice_dataframe: pd.DataFrame):
    result = costs.group_and_sum(
        sample_invoice_dataframe,
        (PID, CLUSTER),
        agg_col=COST,
        agg_name="lifetime_allocation_cost",
    )

    decimal_dtype = pd.ArrowDtype(pyarrow.decimal128(21, 2))
    expected = pd.DataFrame(
        {
            PID: ["vllm-test", "webrca-1b021a"],
            CLUSTER: ["ocp-test", "ocp-prod"],
            "lifetime_allocation_cost": pd.array(
                [Decimal("3.58"), Decimal("0.00")], dtype=decimal_dtype
            ),
        }
    )
    assert result.equals(expected)


@pytest.mark.parametrize(
    "invalid_filters",
    [
        {PID: "does-not-exist"},
        {CLUSTER: "not-a-real-cluster"},
        {PID: "does-not-exist", CLUSTER: "not-a-real-cluster"},
    ],
)
def test_calculate_lifetime_costs_invalid_queries_return_empty(
    monkeypatch: pytest.MonkeyPatch, invalid_filters: dict[str, str]
):
    empty_df = pd.DataFrame(columns=[PID, CLUSTER, COST])
    monkeypatch.setattr(costs, "get_invoice_dataframe", lambda cols=None, **f: empty_df)

    result = costs.calculate_lifetime_costs(**invalid_filters)

    expected = pd.DataFrame(columns=[PID, CLUSTER, "lifetime_allocation_cost"])
    assert result.equals(expected)


class _FakeIcebergTable:
    """Responds to .scan().select().to_pandas() chains."""

    def __init__(self, df: pd.DataFrame):
        self._df = df

    def scan(self, row_filter=None):
        return self

    def select(self, *cols):
        return self

    def to_pandas(self):
        return self._df


def test_get_invoice_dataframe_warns_when_no_rows_match(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
):
    table = _FakeIcebergTable(pd.DataFrame(columns=[PID, COST]))
    monkeypatch.setattr(costs, "get_table", lambda: table)

    with caplog.at_level("WARNING", logger=costs.__name__):
        result = costs.get_invoice_dataframe((PID, COST), **{PID: "does-not-exist"})

    assert result.equals(pd.DataFrame(columns=[PID, COST]))
    assert "No invoice rows matched filters" in caplog.text


def test_get_invoice_dataframe_caches_repeated_query(monkeypatch: pytest.MonkeyPatch):
    table = _FakeIcebergTable(pd.DataFrame({PID: ["vllm-test"], COST: [1.0]}))
    mock_get_table = mock.MagicMock(return_value=table)
    monkeypatch.setattr(costs, "get_table", mock_get_table)

    first = costs.get_invoice_dataframe((PID, COST), **{PID: "vllm-test"})
    second = costs.get_invoice_dataframe((PID, COST), **{PID: "vllm-test"})

    assert mock_get_table.call_count == 1
    assert first is second
