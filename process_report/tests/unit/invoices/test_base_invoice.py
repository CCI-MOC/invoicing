from unittest import TestCase, mock
import pandas
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from unittest.mock import MagicMock

from process_report.tests import util as test_utils


class TestBaseInvoice(TestCase):
    def test_filter_exported_columns(self):
        test_invoice = pandas.DataFrame(columns=["C1", "C2", "C3", "C4", "C5"])
        answer_invoice = pandas.DataFrame(columns=["C1", "C3R", "C5R"])
        inv = test_utils.new_base_invoice(data=test_invoice)
        inv.export_data = test_invoice
        inv.export_columns_list = ["C1", "C3", "C5"]
        inv.exported_columns_map = {"C3": "C3R", "C5": "C5R"}
        inv._filter_columns()
        result_invoice = inv.export_data

        self.assertTrue(result_invoice.equals(answer_invoice))

    @mock.patch("pandas.read_csv")
    def test_fetch_with_mock_s3_bucket(self, mock_read_csv: "MagicMock") -> None:
        """Test that fetch() method loads data correctly when S3 bucket is mocked."""
        test_invoice = test_utils.new_base_invoice(
            name="TestInvoice", invoice_month="2024-08"
        )
        expected_data = pandas.DataFrame(
            {
                "Invoice Month": ["2024-08", "2024-08"],
                "Project - Allocation": ["P1", "P2"],
                "Manager (PI)": ["pi1@bu.edu", "pi2@harvard.edu"],
                "Cost": [100.00, 150.00],
            }
        )
        mock_read_csv.return_value = expected_data

        mock_s3_bucket = mock.MagicMock()
        mock_s3_bucket.download_file.return_value = None

        test_invoice.fetch(mock_s3_bucket)

        self.assertIsNotNone(test_invoice.data)
        self.assertTrue(test_invoice.data.equals(expected_data))
        mock_s3_bucket.download_file.assert_called_once_with(
            "Invoices/2024-08/TestInvoice 2024-08.csv", "TestInvoice 2024-08.csv"
        )


class TestUploadToS3(TestCase):
    @mock.patch("process_report.util.get_invoice_bucket")
    @mock.patch("process_report.util.get_iso8601_time")
    def test_upload_to_s3(self, mock_get_time, mock_get_bucket):
        mock_bucket = mock.MagicMock()
        mock_get_bucket.return_value = mock_bucket
        mock_get_time.return_value = "0"

        invoice_month = "2024-03"
        filenames = ["test-test", "test2.test", "test3"]
        sample_base_invoice = test_utils.new_base_invoice(invoice_month=invoice_month)

        answers = [
            (
                f"test-test {invoice_month}.csv",
                f"Invoices/{invoice_month}/test-test {invoice_month}.csv",
            ),
            (
                f"test-test {invoice_month}.csv",
                f"Invoices/{invoice_month}/Archive/test-test {invoice_month} 0.csv",
            ),
            (
                f"test2.test {invoice_month}.csv",
                f"Invoices/{invoice_month}/test2.test {invoice_month}.csv",
            ),
            (
                f"test2.test {invoice_month}.csv",
                f"Invoices/{invoice_month}/Archive/test2.test {invoice_month} 0.csv",
            ),
            (
                f"test3 {invoice_month}.csv",
                f"Invoices/{invoice_month}/test3 {invoice_month}.csv",
            ),
            (
                f"test3 {invoice_month}.csv",
                f"Invoices/{invoice_month}/Archive/test3 {invoice_month} 0.csv",
            ),
        ]

        for filename in filenames:
            sample_base_invoice.name = filename
            sample_base_invoice.export_s3(mock_bucket)

        for i, call_args in enumerate(mock_bucket.upload_file.call_args_list):
            self.assertTrue(answers[i] in call_args)
