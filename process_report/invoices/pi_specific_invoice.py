import os
import sys
from dataclasses import dataclass
import subprocess
import tempfile
import logging

import pandas
from jinja2 import Environment, FileSystemLoader

import process_report.invoices.invoice as invoice
import process_report.util as util


TEMPLATE_DIR_PATH = "process_report/templates"
CHROME_BIN_PATH = os.environ.get("CHROME_BIN_PATH", "/usr/bin/chromium")


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass
class PIInvoice(invoice.Invoice):
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

        # Copy the first row and modify values to keep row formatting
        totals_row = pi_projects.iloc[[0]].copy()
        # Clear all values to empty strings
        for col in totals_row.columns:
            totals_row[col] = ""

        totals_row[invoice.INVOICE_DATE_FIELD] = "Total"
        for col, sum_val in zip(sum_columns_list, column_sums):
            totals_row[col] = sum_val

        pi_projects = pandas.concat([pi_projects, totals_row], ignore_index=True)

        # Add dollar sign to certain columns
        for column_name in self.DOLLAR_COLUMN_LIST:
            if column_name in pi_projects.columns:
                pi_projects[column_name] = pi_projects[column_name].apply(
                    lambda data: data if pandas.isna(data) else f"${data}"
                )

        # Convert to StringDtype for template compatibility before filling NA values
        pi_projects = pi_projects.astype(pandas.StringDtype())

        # Convert any remaining pandas NA values to empty strings for template compatibility
        pi_projects = pi_projects.fillna("")

        return pi_projects

    def export(self):
        def _create_html_invoice(temp_fd):
            environment = Environment(loader=FileSystemLoader(TEMPLATE_DIR_PATH))
            template = environment.get_template("pi_invoice.html")
            content = template.render(
                data=pi_dataframe,
            )
            temp_fd.write(content)
            temp_fd.flush()

        def _create_pdf_invoice(temp_fd_name):
            if not os.path.exists(CHROME_BIN_PATH):
                sys.exit(
                    f"Chrome binary does not exist at {CHROME_BIN_PATH}. Make sure the env var CHROME_BIN_PATH is set correctly and that Google Chrome is installed"
                )

            invoice_pdf_path = (
                f"{self.name}/{pi_instituition}_{pi}_{self.invoice_month}.pdf"
            )
            subprocess.run(
                [
                    CHROME_BIN_PATH,
                    "--headless",
                    "--no-sandbox",
                    f"--print-to-pdf={invoice_pdf_path}",
                    "--no-pdf-header-footer",
                    f"file://{temp_fd_name}",
                ],
                capture_output=True,
            )

        self._filter_columns()

        # self.name is name of folder storing invoices
        os.makedirs(self.name, exist_ok=True)

        for pi in self.pi_list:
            if pandas.isna(pi):
                continue

            pi_dataframe = self._get_pi_dataframe(self.export_data, pi)
            pi_instituition = pi_dataframe[invoice.INSTITUTION_FIELD].iat[0]

            with tempfile.NamedTemporaryFile(mode="w", suffix=".html") as temp_fd:
                _create_html_invoice(temp_fd)
                _create_pdf_invoice(temp_fd.name)

    def export_s3(self, s3_bucket):
        def _export_s3_pi_invoice(pi_invoice):
            pi_invoice_path = os.path.join(self.name, pi_invoice)
            striped_invoice_path = os.path.splitext(pi_invoice_path)[0]
            output_s3_path = f"Invoices/{self.invoice_month}/{striped_invoice_path}.pdf"
            output_s3_archive_path = f"Invoices/{self.invoice_month}/Archive/{striped_invoice_path} {util.get_iso8601_time()}.pdf"
            s3_bucket.upload_file(pi_invoice_path, output_s3_path)
            s3_bucket.upload_file(pi_invoice_path, output_s3_archive_path)

        for pi_invoice in os.listdir(self.name):
            _export_s3_pi_invoice(pi_invoice)
