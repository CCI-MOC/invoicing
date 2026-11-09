import tempfile
from unittest import TestCase
from process_report.invoices import invoice
import pandas

from process_report import invoice_csv_io


class TestInvoiceCSV(TestCase):
    def _round_trip(self, df):
        with tempfile.NamedTemporaryFile(suffix=".csv") as f:
            invoice_csv_io.write_invoice_csv(df, f.name)
            return invoice_csv_io.read_invoice_csv(f.name)

    def test_round_trip_simple(self):
        test_invoice = pandas.DataFrame({"C1": ["proj1", "proj2"], "C2": ["a", "b"]})
        result_invoice = self._round_trip(test_invoice)

        assert result_invoice.equals(test_invoice)

    def test_round_trip_field_with_comma(self):
        test_invoice = pandas.DataFrame({"C1": ["Green, John"], "C2": ["Y"]})
        result_invoice = self._round_trip(test_invoice)

        assert result_invoice.equals(test_invoice)

    def test_reader_casts_invoice_columns(self):
        test_invoice = pandas.DataFrame(
            {
                invoice.INVOICE_DATE_FIELD: ["2025-06"],
                invoice.SU_HOURS_FIELD: [10],
                invoice.RATE_FIELD: ["0.5"],
                invoice.COST_FIELD: ["100.25"],
            }
        )
        answer_invoice = test_invoice.astype(
            {
                invoice.INVOICE_DATE_FIELD: invoice.STRING_FIELD_TYPE,
                invoice.SU_HOURS_FIELD: invoice.INTEGER_FIELD_TYPE,
                invoice.RATE_FIELD: invoice.RATE_FIELD_TYPE,
                invoice.COST_FIELD: invoice.BALANCE_FIELD_TYPE,
            }
        )
        result_invoice = self._round_trip(test_invoice)

        assert result_invoice.equals(answer_invoice)

    def test_written_file_uses_standard_quotechar(self):
        test_invoice = pandas.DataFrame(
            {invoice.PI_FIELD: ["Green, John"], invoice.COST_FIELD: [100]}
        )
        with tempfile.NamedTemporaryFile(suffix=".csv", mode="r+") as f:
            invoice_csv_io.write_invoice_csv(test_invoice, f.name)
            contents = f.read()

        assert contents == "Manager (PI),Cost\n|Green, John|,100\n"
