from unittest import TestCase, mock
import pandas

from process_report.tests import util as test_utils
from process_report.institute_list_models import InstituteList


class TestNERCTotalInvoice(TestCase):
    def _get_test_invoice(
        self,
        institutions,
        is_billable=None,
        missing_pi=None,
        group_managed=None,
    ):
        if not is_billable:
            is_billable = [True for _ in range(len(institutions))]

        if not missing_pi:
            missing_pi = [False for _ in range(len(institutions))]

        if not group_managed:
            group_managed = [True for _ in range(len(institutions))]

        return pandas.DataFrame(
            {
                "Institution": institutions,
                "Is Billable": is_billable,
                "Missing PI": missing_pi,
                "MGHPCC Managed": group_managed,
            }
        )

    @mock.patch("process_report.util.load_institute_list")
    def test_prepare_export(self, mock_load_institute_list):
        """Basic test for coverage of the _prepare_export method."""
        mock_load_institute_list.return_value = InstituteList.model_validate(
            [
                {
                    "display_name": "Institution A",
                    "domains": ["a.edu"],
                    "include_in_nerc_total_invoice": True,
                },
                {
                    "display_name": "Institution B",
                    "domains": ["b.edu"],
                    "include_in_nerc_total_invoice": True,
                },
                {
                    "display_name": "Institution C",
                    "domains": ["c.edu"],
                    "include_in_nerc_total_invoice": False,
                },
            ]
        )

        test_invoice = self._get_test_invoice(
            institutions=["Institution A", "Institution B", "Institution C"],
            group_managed=[False] * 3,  # Institution C should not be included
        )
        answer_invoice = test_invoice.copy()[0:2]

        test_inv = test_utils.new_nerc_total_invoice(
            data=test_invoice,
            invoice_month="2025-01",
        )
        test_inv._prepare_export()
        output_invoice = test_inv.export_data
        assert output_invoice.equals(answer_invoice)
