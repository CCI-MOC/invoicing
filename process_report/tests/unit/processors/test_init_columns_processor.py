from unittest import TestCase

import pandas as pd

from process_report.processors.init_columns_processor import PISUCreditProcessor


class TestInitColumnsProcessor(TestCase):
    def test_adds_missing_columns_and_sets_balance_and_pi_balance_from_cost(self):
        invoice_month = "2025-01"
        test_data = pd.DataFrame({"Invoice Month": [invoice_month], "Cost": [100.0]})

        processor = PISUCreditProcessor(invoice_month=invoice_month, data=test_data)
        processor.process()

        expected_columns = [
            "Invoice Month",
            "Project - Allocation",
            "Project - Allocation ID",
            "Manager (PI)",
            "Invoice Email",
            "Invoice Address",
            "Institution",
            "Institution - Specific Code",
            "Prepaid Group Name",
            "Prepaid Group Institution",
            "Prepaid Group Balance",
            "Prepaid Group Used",
            "SU Hours (GBhr or SUhr)",
            "SU Type",
            "SU Charge",
            "Charge",
            "Rate",
            "Cost",
            "Credit",
            "Credit Code",
            "Subsidy",
            "Balance",
            "Is Billable",
            "Missing PI",
            "PI Balance",
            "Project",
            "MGHPCC Managed",
            "Cluster Name",
            "Is Course",
        ]

        output_data = processor.data

        # All expected invoice fields are present after processing.
        self.assertEqual(set(expected_columns), set(output_data.columns))

        # Balance fields should equal the Cost column.
        assert output_data["Cost"].equals(output_data["PI Balance"])
        assert output_data["Cost"].equals(output_data["Balance"])
