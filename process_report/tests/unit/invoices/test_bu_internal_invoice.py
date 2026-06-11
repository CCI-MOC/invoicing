from unittest import TestCase
import pandas

from process_report.tests import util as test_utils


class TestBuInternalInvoice(TestCase):
    def _get_test_invoice(
        self,
        institutions,
        projects,
        costs,
        is_billable=None,
        missing_pi=None,
        credits=None,
        subsidies=None,
        pi_balances=None,
    ):
        if is_billable is None:
            is_billable = [True for _ in range(len(institutions))]

        if missing_pi is None:
            missing_pi = [False for _ in range(len(institutions))]

        if credits is None:
            credits = [0 for _ in range(len(institutions))]
        if subsidies is None:
            subsidies = [0 for _ in range(len(institutions))]
        if pi_balances is None:
            pi_balances = costs[:]

        return pandas.DataFrame(
            {
                "Institution": institutions,
                "Is Billable": is_billable,
                "Missing PI": missing_pi,
                "Project": projects,
                "Cost": costs,
                "Credit": credits,
                "Subsidy": subsidies,
                "PI Balance": pi_balances,
            }
        )

    def test_prepare_export(self):
        test_invoice = self._get_test_invoice(
            institutions=[
                "Boston University",
                "MIT",
                "Boston University",
                "Boston University",
            ],
            projects=["ProjectA", "ProjectB", "ProjectC", "ProjectD"],
            costs=[100, 200, 300, 400],
            is_billable=[True, True, False, True],
            missing_pi=[False, False, False, True],
        )
        inv = test_utils.new_bu_internal_invoice(data=test_invoice)
        inv._prepare_export()

    def test_sum_project_allocations(self):
        test_invoice = self._get_test_invoice(
            institutions=["Boston University", "Boston University"],
            projects=["ProjectA", "ProjectA"],
            costs=[100, 200],
            credits=[10, 20],
            subsidies=[5, 5],
            pi_balances=[90, 180],
        )
        inv = test_utils.new_bu_internal_invoice(data=test_invoice)
        inv._sum_project_allocations(test_invoice)
