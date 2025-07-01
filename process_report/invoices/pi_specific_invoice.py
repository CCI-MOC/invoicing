import os
from dataclasses import dataclass
import tempfile
import logging

import pandas

from process_report.invoices import invoice, pdf_invoice


TEMPLATE_DIR_PATH = "process_report/templates"


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass
class PIInvoice(pdf_invoice.PDFInvoice):
    """
    This invoice operates on data processed by these Processors:
    - ValidateBillablePIsProcessor
    - NewPICreditProcessor
    """

    TOTAL_COLUMN_LIST = [
        invoice.COST_FIELD,
        invoice.CREDIT_FIELD,
        invoice.BALANCE_FIELD,
    ]

    DOLLAR_COLUMN_LIST = [
        invoice.RATE_FIELD,
        invoice.GROUP_BALANCE_FIELD,
        invoice.COST_FIELD,
        invoice.GROUP_BALANCE_USED_FIELD,
        invoice.CREDIT_FIELD,
        invoice.BALANCE_FIELD,
    ]

    export_columns_list = [
        invoice.INVOICE_DATE_FIELD,
        invoice.PROJECT_FIELD,
        invoice.PROJECT_ID_FIELD,
        invoice.PI_FIELD,
        invoice.INVOICE_EMAIL_FIELD,
        invoice.INVOICE_ADDRESS_FIELD,
        invoice.INSTITUTION_FIELD,
        invoice.INSTITUTION_ID_FIELD,
        invoice.SU_HOURS_FIELD,
        invoice.SU_TYPE_FIELD,
        invoice.RATE_FIELD,
        invoice.GROUP_NAME_FIELD,
        invoice.GROUP_INSTITUTION_FIELD,
        invoice.GROUP_BALANCE_FIELD,
        invoice.COST_FIELD,
        invoice.GROUP_BALANCE_USED_FIELD,
        invoice.CREDIT_FIELD,
        invoice.CREDIT_CODE_FIELD,
        invoice.BALANCE_FIELD,
    ]

    def _prepare(self):
        self.export_data = self.data[
            self.data[invoice.IS_BILLABLE_FIELD] & ~self.data[invoice.MISSING_PI_FIELD]
        ]
        self.pi_list = self.export_data[invoice.PI_FIELD].unique()

    def _get_pi_dataframe(self, data, pi):
        pi_projects = data[data[invoice.PI_FIELD] == pi].copy().reset_index(drop=True)

        # Remove prepay group data if it's empty
        if pandas.isna(pi_projects[invoice.GROUP_NAME_FIELD]).all():
            pi_projects = pi_projects.drop(
                [
                    invoice.GROUP_NAME_FIELD,
                    invoice.GROUP_INSTITUTION_FIELD,
                    invoice.GROUP_BALANCE_FIELD,
                    invoice.GROUP_BALANCE_USED_FIELD,
                ],
                axis=1,
            )

        # Add a row containing sums for certain columns
        column_sums = []
        sum_columns_list = []
        for column_name in self.TOTAL_COLUMN_LIST:
            if column_name in pi_projects.columns:
                column_sums.append(pi_projects[column_name].sum())
                sum_columns_list.append(column_name)
        pi_projects.loc[len(pi_projects)] = (
            None  # Adds a new row to end of dataframe initialized with None
        )
        pi_projects.loc[pi_projects.index[-1], invoice.INVOICE_DATE_FIELD] = "Total"
        pi_projects.loc[pi_projects.index[-1], sum_columns_list] = column_sums

        # Add dollar sign to certain columns
        for column_name in self.DOLLAR_COLUMN_LIST:
            if column_name in pi_projects.columns:
                pi_projects[column_name] = pi_projects[column_name].apply(
                    lambda data: data if pandas.isna(data) else f"${data}"
                )

        pi_projects.fillna("", inplace=True)

        return pi_projects

    def export(self):
        self._filter_columns()

        # self.name is name of folder storing invoices
        os.makedirs(self.name, exist_ok=True)

        for pi in self.pi_list:
            if pandas.isna(pi):
                continue

            pi_dataframe = self._get_pi_dataframe(self.export_data, pi)
            pi_instituition = pi_dataframe[invoice.INSTITUTION_FIELD].iat[0]
            invoice_pdf_path = (
                f"{self.name}/{pi_instituition}_{pi}_{self.invoice_month}.pdf"
            )

            with tempfile.NamedTemporaryFile(mode="w", suffix=".html") as temp_fd:
                self._create_html_invoice(temp_fd, pi_dataframe, "pi_invoice.html")
                self._create_pdf_invoice(temp_fd.name, invoice_pdf_path)
