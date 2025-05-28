from unittest import TestCase, mock
import pandas
from decimal import Decimal

from process_report.institute_list_models import InstituteList
from process_report.tests import util as test_utils
from process_report.tests.base import BaseTestCaseWithTempDir


class TestNERCRates(TestCase):
    @mock.patch("process_report.util.load_institute_list")
    def test_flag_limit_new_pi_credit(self, mock_load_institute_list):
        mock_load_institute_list.return_value = InstituteList(
            [
                {
                    "domains": [],
                    "display_name": "BU",
                    "mghpcc_partnership_start_date": "2024-02",
                },
                {
                    "domains": [],
                    "display_name": "HU",
                    "mghpcc_partnership_start_date": "2024-6",
                },
                {
                    "domains": [],
                    "display_name": "NEU",
                    "mghpcc_partnership_start_date": "2024-11",
                },
            ]
        )
        sample_df = pandas.DataFrame(
            {
                "Institution": ["BU", "HU", "NEU", "MIT", "BC"],
            }
        )
        sample_proc = test_utils.new_new_pi_credit_processor(
            limit_new_pi_credit_to_partners=True
        )

        # When no partnerships are active
        sample_proc.invoice_month = "2024-01"
        output_df = sample_proc._filter_partners(sample_df)
        self.assertTrue(output_df.empty)

        # When some partnerships are active
        sample_proc.invoice_month = "2024-06"
        output_df = sample_proc._filter_partners(sample_df)
        answer_df = pandas.DataFrame({"Institution": ["BU", "HU"]})
        self.assertTrue(output_df.equals(answer_df))

        # When all partnerships are active
        sample_proc.invoice_month = "2024-12"
        output_df = sample_proc._filter_partners(sample_df)
        answer_df = pandas.DataFrame({"Institution": ["BU", "HU", "NEU"]})
        self.assertTrue(output_df.equals(answer_df))


class TestNewPICreditProcessor(BaseTestCaseWithTempDir):
    def _assert_result_invoice_and_old_pi_file(
        self,
        invoice_month,
        test_invoice,
        test_old_pi_filepath,
        answer_invoice,
        answer_old_pi_df,
    ):
        new_pi_credit_proc = test_utils.new_new_pi_credit_processor(
            invoice_month=invoice_month,
            data=test_invoice,
            old_pi_filepath=test_old_pi_filepath,
        )
        new_pi_credit_proc.process()
        output_invoice = new_pi_credit_proc.data
        output_old_pi_df = new_pi_credit_proc.updated_old_pi_df.sort_values(
            by="PI", ignore_index=True
        )

        answer_invoice = answer_invoice.astype(output_invoice.dtypes)
        answer_old_pi_df = answer_old_pi_df.astype(output_old_pi_df.dtypes).sort_values(
            by="PI", ignore_index=True
        )

        self.assertTrue(output_invoice.equals(answer_invoice))
        self.assertTrue(output_old_pi_df.equals(answer_old_pi_df))

    def _get_test_invoice(
        self, pi, cost, su_type=None, is_billable=None, missing_pi=None
    ):
        if not su_type:
            su_type = ["CPU" for _ in range(len(pi))]

        if not is_billable:
            is_billable = [True for _ in range(len(pi))]

        if not missing_pi:
            missing_pi = [False for _ in range(len(pi))]

        return pandas.DataFrame(
            {
                "Manager (PI)": pi,
                "Cost": cost,
                "SU Type": su_type,
                "Is Billable": is_billable,
                "Missing PI": missing_pi,
            }
        )

    def test_no_new_pi(self):
        test_invoice = self._get_test_invoice(
            ["PI" for _ in range(3)], [Decimal("100.0") for _ in range(3)]
        )
        test_old_pi_file = self.tempdir / "old_pi.csv"

        # Other fields of old PI file not accessed if PI is no longer
        # eligible for new-PI credit
        test_old_pi_df = pandas.DataFrame(
            {
                "PI": ["PI"],
                "First Invoice Month": ["2024-01"],
                "Initial Credits": [Decimal("1000.0")],
            }
        )
        test_old_pi_df.to_csv(test_old_pi_file, index=False)

        answer_invoice = pandas.concat(
            [
                test_invoice,
                pandas.DataFrame(
                    {
                        "Credit": [None for _ in range(3)],
                        "Credit Code": [None for _ in range(3)],
                        "PI Balance": [Decimal("100.0") for _ in range(3)],
                        "Balance": [Decimal("100.0") for _ in range(3)],
                    }
                ),
            ],
            axis=1,
        )

        answer_old_pi_df = test_old_pi_df.copy()

        self._assert_result_invoice_and_old_pi_file(
            "2024-06",
            test_invoice,
            str(test_old_pi_file),
            answer_invoice,
            answer_old_pi_df,
        )

    def test_one_new_pi(self):
        """Invoice with one completely new PI"""

        # One allocation
        invoice_month = "2024-06"
        test_invoice = self._get_test_invoice(["PI"], [Decimal("100.0")])
        test_old_pi_file = self.tempdir / "old_pi.csv"
        test_old_pi_df = pandas.DataFrame(
            columns=[
                "PI",
                "First Invoice Month",
                "Initial Credits",
                "1st Month Used",
                "2nd Month Used",
            ]
        )
        test_old_pi_df.to_csv(test_old_pi_file, index=False)

        answer_invoice = pandas.concat(
            [
                test_invoice,
                pandas.DataFrame(
                    {
                        "Credit": [Decimal("100.0")],
                        "Credit Code": ["0002"],
                        "PI Balance": [Decimal("0.0")],
                        "Balance": [Decimal("0.0")],
                    }
                ),
            ],
            axis=1,
        )

        answer_old_pi_df = pandas.DataFrame(
            {
                "PI": ["PI"],
                "First Invoice Month": ["2024-06"],
                "Initial Credits": [Decimal("1000.0")],
                "1st Month Used": [Decimal("100.0")],
                "2nd Month Used": [Decimal("0.0")],
            }
        )

        self._assert_result_invoice_and_old_pi_file(
            invoice_month,
            test_invoice,
            str(test_old_pi_file),
            answer_invoice,
            answer_old_pi_df,
        )

        # Two allocations, costs partially covered
        test_invoice = self._get_test_invoice(
            ["PI", "PI"], [Decimal("500.0"), Decimal("1000.0")]
        )

        answer_invoice = pandas.concat(
            [
                test_invoice,
                pandas.DataFrame(
                    {
                        "Credit": [Decimal("500.0"), Decimal("500.0")],
                        "Credit Code": ["0002", "0002"],
                        "PI Balance": [Decimal("0.0"), Decimal("500.0")],
                        "Balance": [Decimal("0.0"), Decimal("500.0")],
                    }
                ),
            ],
            axis=1,
        )

        answer_old_pi_df = pandas.DataFrame(
            {
                "PI": ["PI"],
                "First Invoice Month": ["2024-06"],
                "Initial Credits": [Decimal("1000.0")],
                "1st Month Used": [Decimal("1000.0")],
                "2nd Month Used": [Decimal("0.0")],
            }
        )

        self._assert_result_invoice_and_old_pi_file(
            invoice_month,
            test_invoice,
            str(test_old_pi_file),
            answer_invoice,
            answer_old_pi_df,
        )

        # Two allocations, costs completely covered
        test_invoice = self._get_test_invoice(
            ["PI", "PI"], [Decimal("500.0"), Decimal("400.0")]
        )

        answer_invoice = pandas.concat(
            [
                test_invoice,
                pandas.DataFrame(
                    {
                        "Credit": [Decimal("500.0"), Decimal("400.0")],
                        "Credit Code": ["0002", "0002"],
                        "PI Balance": [Decimal("0.0"), Decimal("0.0")],
                        "Balance": [Decimal("0.0"), Decimal("0.0")],
                    }
                ),
            ],
            axis=1,
        )

        answer_old_pi_df = pandas.DataFrame(
            {
                "PI": ["PI"],
                "First Invoice Month": ["2024-06"],
                "Initial Credits": [Decimal("1000.0")],
                "1st Month Used": [Decimal("900.0")],
                "2nd Month Used": [Decimal("0.0")],
            }
        )

        self._assert_result_invoice_and_old_pi_file(
            invoice_month,
            test_invoice,
            str(test_old_pi_file),
            answer_invoice,
            answer_old_pi_df,
        )

    def test_one_month_pi(self):
        """PI has appeared in invoices for one month"""

        # Remaining credits completely covers costs
        invoice_month = "2024-07"
        test_invoice = self._get_test_invoice(["PI"], [Decimal("200.0")])
        test_old_pi_file = self.tempdir / "old_pi.csv"
        test_old_pi_df = pandas.DataFrame(
            {
                "PI": ["PI"],
                "First Invoice Month": ["2024-06"],
                "Initial Credits": [Decimal("1000.0")],
                "1st Month Used": [Decimal("500.0")],
                "2nd Month Used": [Decimal("0.0")],
            }
        )
        test_old_pi_df.to_csv(test_old_pi_file, index=False)

        answer_invoice = pandas.concat(
            [
                test_invoice,
                pandas.DataFrame(
                    {
                        "Credit": [Decimal("200.0")],
                        "Credit Code": ["0002"],
                        "PI Balance": [Decimal("0.0")],
                        "Balance": [Decimal("0.0")],
                    }
                ),
            ],
            axis=1,
        )

        answer_old_pi_df = pandas.DataFrame(
            {
                "PI": ["PI"],
                "First Invoice Month": ["2024-06"],
                "Initial Credits": [Decimal("1000.0")],
                "1st Month Used": [Decimal("500.0")],
                "2nd Month Used": [Decimal("200.0")],
            }
        )

        self._assert_result_invoice_and_old_pi_file(
            invoice_month,
            test_invoice,
            str(test_old_pi_file),
            answer_invoice,
            answer_old_pi_df,
        )

        # Remaining credits partially covers costs
        test_invoice = self._get_test_invoice(["PI"], [Decimal("600.0")])

        answer_invoice = pandas.concat(
            [
                test_invoice,
                pandas.DataFrame(
                    {
                        "Credit": [Decimal("500.0")],
                        "Credit Code": ["0002"],
                        "PI Balance": [Decimal("100.0")],
                        "Balance": [Decimal("100.0")],
                    }
                ),
            ],
            axis=1,
        )

        answer_old_pi_df = pandas.DataFrame(
            {
                "PI": ["PI"],
                "First Invoice Month": ["2024-06"],
                "Initial Credits": [Decimal("1000.0")],
                "1st Month Used": [Decimal("500.0")],
                "2nd Month Used": [Decimal("500.0")],
            }
        )

        self._assert_result_invoice_and_old_pi_file(
            invoice_month,
            test_invoice,
            str(test_old_pi_file),
            answer_invoice,
            answer_old_pi_df,
        )

    def test_two_new_pi(self):
        """Two PIs of different age"""

        # Costs partially and completely covered
        invoice_month = "2024-07"
        test_invoice = self._get_test_invoice(
            ["PI1", "PI1", "PI2"],
            [Decimal("800.0"), Decimal("500.0"), Decimal("500.0")],
        )
        test_old_pi_file = self.tempdir / "old_pi.csv"
        test_old_pi_df = pandas.DataFrame(
            {
                "PI": ["PI1"],
                "First Invoice Month": ["2024-06"],
                "Initial Credits": [Decimal("1000.0")],
                "1st Month Used": [Decimal("500.0")],
                "2nd Month Used": [Decimal("0.0")],
            }
        )
        test_old_pi_df.to_csv(test_old_pi_file, index=False)

        answer_invoice = pandas.concat(
            [
                test_invoice,
                pandas.DataFrame(
                    {
                        "Credit": [Decimal("500.0"), None, Decimal("500.0")],
                        "Credit Code": ["0002", None, "0002"],
                        "PI Balance": [
                            Decimal("300.0"),
                            Decimal("500.0"),
                            Decimal("0.0"),
                        ],
                        "Balance": [Decimal("300.0"), Decimal("500.0"), Decimal("0.0")],
                    }
                ),
            ],
            axis=1,
        )

        answer_old_pi_df = pandas.DataFrame(
            {
                "PI": ["PI1", "PI2"],
                "First Invoice Month": ["2024-06", "2024-07"],
                "Initial Credits": [Decimal("1000.0"), Decimal("1000.0")],
                "1st Month Used": [Decimal("500.0"), Decimal("500.0")],
                "2nd Month Used": [Decimal("500.0"), Decimal("0.0")],
            }
        )

        self._assert_result_invoice_and_old_pi_file(
            invoice_month,
            test_invoice,
            str(test_old_pi_file),
            answer_invoice,
            answer_old_pi_df,
        )

    def test_old_pi_file_overwritten(self):
        """If PI already has entry in Old PI file,
        their initial credits and PI entry could be overwritten"""

        invoice_month = "2024-06"
        test_invoice = self._get_test_invoice(
            ["PI", "PI"], [Decimal("500.0"), Decimal("500.0")]
        )
        test_old_pi_file = self.tempdir / "old_pi.csv"
        test_old_pi_df = pandas.DataFrame(
            {
                "PI": ["PI"],
                "First Invoice Month": ["2024-06"],
                "Initial Credits": [Decimal("500.0")],
                "1st Month Used": [Decimal("200.0")],
                "2nd Month Used": [Decimal("0.0")],
            }
        )
        test_old_pi_df.to_csv(test_old_pi_file, index=False)

        answer_invoice = pandas.concat(
            [
                test_invoice,
                pandas.DataFrame(
                    {
                        "Credit": [Decimal("500.0"), None],
                        "Credit Code": ["0002", None],
                        "PI Balance": [Decimal("0.0"), Decimal("500.0")],
                        "Balance": [Decimal("0.0"), Decimal("500.0")],
                    }
                ),
            ],
            axis=1,
        )

        answer_old_pi_df = pandas.DataFrame(
            {
                "PI": ["PI"],
                "First Invoice Month": ["2024-06"],
                "Initial Credits": [Decimal("500.0")],
                "1st Month Used": [Decimal("500.0")],
                "2nd Month Used": [Decimal("0.0")],
            }
        )

        self._assert_result_invoice_and_old_pi_file(
            invoice_month,
            test_invoice,
            str(test_old_pi_file),
            answer_invoice,
            answer_old_pi_df,
        )

    def test_excluded_su_types(self):
        """Certain SU types can be excluded from the credit"""

        invoice_month = "2024-06"
        test_invoice = self._get_test_invoice(
            ["PI", "PI", "PI", "PI"],
            [Decimal("600.0"), Decimal("600.0"), Decimal("600.0"), Decimal("600.0")],
            [
                "CPU",
                "OpenShift GPUA100SXM4",
                "GPU",
                "OpenStack GPUA100SXM4",
            ],
        )
        test_old_pi_file = self.tempdir / "old_pi.csv"
        test_old_pi_df = pandas.DataFrame(
            columns=[
                "PI",
                "First Invoice Month",
                "Initial Credits",
                "1st Month Used",
                "2nd Month Used",
            ]
        )
        test_old_pi_df.to_csv(test_old_pi_file, index=False)

        answer_invoice = pandas.concat(
            [
                test_invoice,
                pandas.DataFrame(
                    {
                        "Credit": [Decimal("600.0"), None, Decimal("400.0"), None],
                        "Credit Code": ["0002", None, "0002", None],
                        "PI Balance": [
                            Decimal("0.0"),
                            Decimal("600.0"),
                            Decimal("200.0"),
                            Decimal("600.0"),
                        ],
                        "Balance": [
                            Decimal("0.0"),
                            Decimal("600.0"),
                            Decimal("200.0"),
                            Decimal("600.0"),
                        ],
                    }
                ),
            ],
            axis=1,
        )

        answer_old_pi_df = pandas.DataFrame(
            {
                "PI": ["PI"],
                "First Invoice Month": ["2024-06"],
                "Initial Credits": [Decimal("1000.0")],
                "1st Month Used": [Decimal("1000.0")],
                "2nd Month Used": [Decimal("0.0")],
            }
        )

        self._assert_result_invoice_and_old_pi_file(
            invoice_month,
            test_invoice,
            str(test_old_pi_file),
            answer_invoice,
            answer_old_pi_df,
        )

    def test_apply_credit_error(self):
        """Test faulty data"""
        old_pi_df = pandas.DataFrame(
            {"PI": ["PI1"], "First Invoice Month": ["2024-04"]}
        )
        invoice_month = "2024-03"
        test_invoice = test_utils.new_new_pi_credit_processor()
        with self.assertRaises(SystemExit):
            test_invoice._get_pi_age(old_pi_df, "PI1", invoice_month)
