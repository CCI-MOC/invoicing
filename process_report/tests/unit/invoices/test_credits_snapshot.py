from unittest import TestCase
import pandas

from process_report.tests import util as test_utils


class TestCreditsSnapshot(TestCase):
    def _get_test_prepay_credits(self, months, group_names, credits):
        return pandas.DataFrame(
            {"Month": months, "Group Name": group_names, "Credit": credits}
        )

    def _get_test_prepay_contacts(self, group_names, emails, is_managed):
        return pandas.DataFrame(
            {
                "Group Name": group_names,
                "Group Contact Email": emails,
                "MGHPCC Managed": is_managed,
            }
        )

    def test_get_credit_snapshot(self):
        invoice_month = "2024-10"
        test_prepay_credits = self._get_test_prepay_credits(
            ["2024-10", "2024-10", "2024-10", "2024-09", "2024-09"],
            ["G1", "G2", "G3", "G1", "G2"],
            [0] * 5,
        )
        test_prepay_contacts = self._get_test_prepay_contacts(
            ["G1", "G2", "G3"], [""] * 3, ["Yes", "No", "Yes"]
        )
        answer_credits_snapshot = test_prepay_credits.iloc[[0, 2]]

        new_prepayment_proc = test_utils.new_prepay_credits_snapshot(
            invoice_month=invoice_month,
            prepay_credits=test_prepay_credits,
            prepay_contacts=test_prepay_contacts,
        )
        output_snapshot = new_prepayment_proc._get_prepay_credits_snapshot()

        assert answer_credits_snapshot.equals(output_snapshot)
