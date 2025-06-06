from unittest import TestCase, mock
import tempfile
import pandas

from process_report.tests import util as test_utils


class TestMOCAGroupSpecificInvoice(TestCase):
    @staticmethod
    def _add_dollar_sign(data):
        if pandas.isna(data):
            return data
        else:
            return "$" + str(data)

    def _get_test_invoice(
        self,
        project,
        balance=0,
        is_billable=True,
        missing_pi=False,
        group_name=None,
        group_contact=None,
        group_institution=None,
        cost=0,
        group_balance_used=0,
        credits=0,
    ):
        return pandas.DataFrame(
            {
                "Invoice Email": group_contact,
                "Project - Allocation": project,
                "Is Billable": is_billable,
                "Missing PI": missing_pi,
                "Prepaid Group Name": group_name,
                "Prepaid Group Institution": group_institution,
                "Cost": cost,
                "Prepaid Group Used": group_balance_used,
                "Credit": credits,
                "Balance": balance,
            }
        )

    def _get_answer_invoice(self, test_invoice, group_name, sum_row):
        answer_invoice = (
            test_invoice[test_invoice["Prepaid Group Name"] == group_name]
            .copy()
            .reset_index(drop=True)
        )
        answer_invoice.loc[len(answer_invoice)] = None
        answer_invoice.loc[
            answer_invoice.index[-1],
            ["Invoice Month", "Cost", "Prepaid Group Used", "Credit", "Balance"],
        ] = sum_row
        for column_name in [
            "Cost",
            "Prepaid Group Used",
            "Credit",
            "Balance",
        ]:
            answer_invoice[column_name] = answer_invoice[column_name].apply(
                lambda data: data if pandas.isna(data) else f"${data}"
            )
        answer_invoice.fillna("", inplace=True)
        return answer_invoice

    def _add_credit_row(self, invoice, credit_row):
        """Modify input dataframe to add a row for prepay credit."""
        invoice.loc[len(invoice)] = None
        invoice.loc[invoice.index[-1], ["Prepaid Group Name", "Cost", "Balance"]] = (
            credit_row
        )
        return invoice

    def _get_test_prepay_credits(self, months, group_names, credits):
        return pandas.DataFrame(
            {"Month": months, "Group Name": group_names, "Credit": credits}
        )

    def test_filter_rows(self):
        """Are nonbillables and non-group projects correctly filtered out?"""
        test_invoice = self._get_test_invoice(
            ["P1", "P2", "P3", "P4", "P5"],
            is_billable=[True, False, True, True, True],
            missing_pi=[False, False, True, False, False],
            group_name=["G1", None, None, None, "G1"],
        )
        answer_invoice = test_invoice.copy()
        answer_invoice = answer_invoice.iloc[[0, 4]]

        group_inv = test_utils.new_MOCA_group_specific_invoice(data=test_invoice)
        group_inv._prepare()

        self.assertTrue(answer_invoice.equals(group_inv.export_data))

    def test_get_dataframe_one_group(self):
        """One prepay group with three projects, with balance used and credits"""
        invoice_month = "2024-01"
        group_name = "G1"
        test_invoice = self._get_test_invoice(
            ["P1", "P2", "P3"],
            group_name=[group_name] * 3,
            balance=[100, 200, 300],
            group_balance_used=[1000, 2000, 0],
            credits=[50, None, 50],
        )
        test_prepay_credits = self._get_test_prepay_credits(
            [invoice_month], [group_name], [5000]
        )
        answer_invoice = test_invoice.copy()
        answer_invoice = self._add_credit_row(answer_invoice, [group_name, 5000, 5000])
        answer_invoice = self._get_answer_invoice(
            answer_invoice, group_name, ["Total", 5000, 3000, 100, 5600]
        )

        group_inv = test_utils.new_MOCA_group_specific_invoice(
            invoice_month=invoice_month, prepay_credits=test_prepay_credits
        )
        output_invoice = group_inv._get_group_dataframe(test_invoice, group_name)
        self.assertTrue(answer_invoice.equals(output_invoice))

    def test_get_group_dataframe(self):
        """Two prepay groups with one project each, given credits at different times"""

        # Neither groups have credits on current invoice month
        invoice_month = "2024-01"
        group_names = ["G1", "G2"]
        test_prepay_credits = self._get_test_prepay_credits(
            ["2023-12", "2024-02", "2024-03", "2024-03", "2024-03"],
            ["G1", "G1", "G2", "G2", "G1"],
            [1000, 2000, 3000, 4000, 2000],
        )
        test_invoice = self._get_test_invoice(
            ["P1", "P2"],
            group_name=group_names,
            balance=[100, 200],
            group_balance_used=[1000, 2000],
        )

        answer_invoice_G1 = self._get_answer_invoice(
            test_invoice, "G1", sum_row=["Total", 0, 1000, 0, 100]
        )
        answer_invoice_G2 = self._get_answer_invoice(
            test_invoice, "G2", sum_row=["Total", 0, 2000, 0, 200]
        )

        group_inv = test_utils.new_MOCA_group_specific_invoice(
            invoice_month=invoice_month, prepay_credits=test_prepay_credits
        )
        output_invoice = group_inv._get_group_dataframe(test_invoice, "G1")
        self.assertTrue(answer_invoice_G1.equals(output_invoice))

        output_invoice = group_inv._get_group_dataframe(test_invoice, "G2")
        self.assertTrue(answer_invoice_G2.equals(output_invoice))

        # One group has a credit on invoice month. Invoice for G2 is unchanged
        invoice_month = "2024-02"

        answer_invoice_G1 = test_invoice.copy()
        answer_invoice_G1 = self._add_credit_row(
            answer_invoice_G1, credit_row=["G1", 2000, 2000]
        )
        answer_invoice_G1 = self._get_answer_invoice(
            answer_invoice_G1, "G1", sum_row=["Total", 2000, 1000, 0, 2100]
        )

        group_inv.invoice_month = invoice_month
        output_invoice = group_inv._get_group_dataframe(test_invoice, "G1")
        self.assertTrue(answer_invoice_G1.equals(output_invoice))

        output_invoice = group_inv._get_group_dataframe(test_invoice, "G2")
        self.assertTrue(answer_invoice_G2.equals(output_invoice))

        # G2 has two credits, G1 has one credit (should be unchanged)
        invoice_month = "2024-03"

        answer_invoice_G2 = test_invoice.copy()
        answer_invoice_G2 = self._add_credit_row(
            answer_invoice_G2, credit_row=["G2", 3000, 3000]
        )
        answer_invoice_G2 = self._add_credit_row(
            answer_invoice_G2, credit_row=["G2", 4000, 4000]
        )
        answer_invoice_G2 = self._get_answer_invoice(
            answer_invoice_G2, "G2", sum_row=["Total", 7000, 2000, 0, 7200]
        )

        group_inv.invoice_month = invoice_month
        output_invoice = group_inv._get_group_dataframe(test_invoice, "G1")
        self.assertTrue(answer_invoice_G1.equals(output_invoice))

        output_invoice = group_inv._get_group_dataframe(test_invoice, "G2")
        self.assertTrue(answer_invoice_G2.equals(output_invoice))

    @mock.patch("process_report.invoices.invoice.Invoice._filter_columns")
    @mock.patch("os.path.exists")
    @mock.patch("subprocess.run")
    def test_export(self, mock_subprocess_run, mock_path_exists, mock_filter_cols):
        """Are PDFs exported as desired?"""
        invoice_month = "2024-10"
        test_invoice = self._get_test_invoice(
            ["P1", "P2", "P3", "P4"],
            group_name=["G1", "G1", "G2", None],
            group_contact=["G1@bu.edu", "G1@bu.edu", "G2@hu.edu", None],
            group_institution=["BU", "BU", "HU", None],
        )
        test_prepay_credits = self._get_test_prepay_credits([], [], [])

        mock_filter_cols.return_value = test_invoice
        mock_path_exists.return_value = True

        with tempfile.TemporaryDirectory() as test_dir:
            group_inv = test_utils.new_MOCA_group_specific_invoice(
                test_dir,
                invoice_month,
                data=test_invoice,
                prepay_credits=test_prepay_credits,
            )
            group_inv.process()
            group_inv.export()
            group_pdf_1 = f"{test_dir}/BU_G1@bu.edu_{invoice_month}.pdf"
            group_pdf_2 = f"{test_dir}/HU_G2@hu.edu_{invoice_month}.pdf"

            for i, group_pdf_path in enumerate([group_pdf_1, group_pdf_2]):
                chrome_arglist, _ = mock_subprocess_run.call_args_list[i]
                answer_arglist = [
                    "/usr/bin/chromium",
                    "--headless",
                    "--no-sandbox",
                    f"--print-to-pdf={group_pdf_path}",
                    "--no-pdf-header-footer",
                ]
                self.assertTrue(answer_arglist == chrome_arglist[0][:-1])
