import pandas as pd
import pytest

from process_report.data_tools import costs

# These are the column names in the iceberg table using string literals instead of the invoice module to test column name correctness
PID = "Project - Allocation ID"
CLUSTER = "Cluster Name"
BALANCE = "Balance"


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
            BALANCE: [1.234, 2.345, None],
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
    captured: dict[str, object] = {}

    def _fake_loader(cols=None, **filters):
        captured["cols"] = cols
        captured["filters"] = filters
        return sample_invoice_dataframe

    monkeypatch.setattr(costs, "get_invoice_dataframe", _fake_loader)

    result = costs.aggregate_by(
        (BALANCE,),
        (PID, CLUSTER),
        agg_col=BALANCE,
        agg_name="lifetime_allocation_balance",
        **{PID: "vllm-test"},
    )

    assert captured["filters"] == {PID: "vllm-test"}
    assert captured["cols"] == (BALANCE, PID, CLUSTER)

    values = sorted(result["lifetime_allocation_balance"].tolist())
    assert values == [costs.Decimal("0.00"), costs.Decimal("3.58")]
    assert all(v.as_tuple().exponent == -2 for v in values)


def test_group_and_sum_is_pure_transform(sample_invoice_dataframe: pd.DataFrame):
    result = costs.group_and_sum(
        sample_invoice_dataframe,
        (PID, CLUSTER),
        agg_col=BALANCE,
        agg_name="lifetime_allocation_balance",
    )

    values = sorted(result["lifetime_allocation_balance"].tolist())
    assert values == [costs.Decimal("0.00"), costs.Decimal("3.58")]
    assert all(v.as_tuple().exponent == -2 for v in values)


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
    empty_df = pd.DataFrame(columns=[PID, CLUSTER, BALANCE])
    monkeypatch.setattr(costs, "get_invoice_dataframe", lambda cols=None, **f: empty_df)

    result = costs.calculate_lifetime_costs(**invalid_filters)

    assert result.empty
    assert result.columns.tolist() == [PID, CLUSTER, "lifetime_allocation_balance"]


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
    table = _FakeIcebergTable(pd.DataFrame(columns=[PID, BALANCE]))
    monkeypatch.setattr(costs, "get_table", lambda: table)

    with caplog.at_level("WARNING", logger=costs.__name__):
        result = costs.get_invoice_dataframe((PID, BALANCE), **{PID: "does-not-exist"})

    assert result.empty
    assert "No invoice rows matched filters" in caplog.text


def test_get_invoice_dataframe_caches_repeated_query(monkeypatch: pytest.MonkeyPatch):
    table = _FakeIcebergTable(pd.DataFrame({PID: ["vllm-test"], BALANCE: [1.0]}))
    call_counter = {"count": 0}

    def _fake_get_table():
        call_counter["count"] += 1
        return table

    monkeypatch.setattr(costs, "get_table", _fake_get_table)

    first = costs.get_invoice_dataframe((PID, BALANCE), **{PID: "vllm-test"})
    second = costs.get_invoice_dataframe((PID, BALANCE), **{PID: "vllm-test"})

    assert call_counter["count"] == 1
    assert first is second
