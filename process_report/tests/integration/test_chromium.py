import os
from typing import override

import pandas as pd

from process_report.invoices.pi_specific_invoice import PIInvoice
from process_report.invoices import invoice
from process_report.tests.base import BaseTestCaseWithTempDir


class TestPIInvoiceExport(BaseTestCaseWithTempDir):
    invoice_month: str = ""
    df: pd.DataFrame = pd.DataFrame()

    @override
    def setUp(self):
        super().setUp()
        self.invoice_month = "2024-01"

        self.df = pd.DataFrame(
            [
                {
                    invoice.INVOICE_DATE_FIELD: "2024-01-01",
                    invoice.PROJECT_FIELD: "ProjectX",
                    invoice.PROJECT_ID_FIELD: "PA-001",
                    invoice.PI_FIELD: "Jane Doe",
                    invoice.INVOICE_EMAIL_FIELD: "jane.doe@university.edu",
                    invoice.INVOICE_ADDRESS_FIELD: "123 Campus Dr, Cityville",
                    invoice.INSTITUTION_FIELD: "Test University",
                    invoice.INSTITUTION_ID_FIELD: "TU-001",
                    invoice.SU_HOURS_FIELD: 50,
                    invoice.SU_TYPE_FIELD: "GBhr",
                    invoice.RATE_FIELD: 2.0,
                    invoice.GROUP_NAME_FIELD: "PrepaidGroupX",
                    invoice.GROUP_INSTITUTION_FIELD: "Test University",
                    invoice.GROUP_BALANCE_FIELD: 1000,
                    invoice.COST_FIELD: 100,
                    invoice.GROUP_BALANCE_USED_FIELD: 200,
                    invoice.CREDIT_FIELD: 20,
                    invoice.CREDIT_CODE_FIELD: "CRED123",
                    invoice.BALANCE_FIELD: 80,
                    invoice.IS_BILLABLE_FIELD: True,
                    invoice.MISSING_PI_FIELD: False,
                }
            ]
        )

    def test_pi_invoice_pdf_generation(self):
        pi_invoice = PIInvoice(
            name=str(self.tempdir), invoice_month=self.invoice_month, data=self.df
        )
        pi_invoice.process()
        pi_invoice.export()

        output_pdfs = os.listdir(self.tempdir)

        self.assertEqual(["Test University_Jane Doe_2024-01.pdf"], output_pdfs)

        # Validate the PDF header
        for pdf in output_pdfs:
            pdf_path = os.path.join(self.tempdir, pdf)
            with open(pdf_path, "rb") as f:
                header = f.read(4)
            self.assertEqual(header, b"%PDF")
