import os
import sys
from dataclasses import dataclass
import subprocess

import pandas
from jinja2 import Environment, FileSystemLoader

import process_report.invoices.invoice as invoice
import process_report.util as util


TEMPLATE_DIR_PATH = "process_report/templates"


@dataclass
class PDFInvoice(invoice.Invoice):
    @staticmethod
    def _create_html_invoice(temp_fd, data: pandas.DataFrame, template_filename: str):
        environment = Environment(loader=FileSystemLoader(TEMPLATE_DIR_PATH))
        template = environment.get_template(template_filename)
        content = template.render(
            data=data,
        )
        temp_fd.write(content)
        temp_fd.flush()

    @staticmethod
    def _create_pdf_invoice(html_filepath: str, output_pdf_path: str):
        chrome_binary_location = os.environ.get("CHROME_BIN_PATH", "/usr/bin/chromium")
        if not os.path.exists(chrome_binary_location):
            sys.exit(
                f"Chrome binary does not exist at {chrome_binary_location}. Make sure the env var CHROME_BIN_PATH is set correctly or that Google Chrome is installed"
            )

        subprocess.run(
            [
                chrome_binary_location,
                "--headless",
                "--no-sandbox",
                f"--print-to-pdf={output_pdf_path}",
                "--no-pdf-header-footer",
                "file://" + html_filepath,
            ],
            capture_output=True,
        )

    def export_s3(self, s3_bucket):
        def _export_s3_group_invoice(invoice):
            invoice_path = os.path.join(self.name, invoice)
            striped_invoice_path = os.path.splitext(invoice_path)[0]
            output_s3_path = f"Invoices/{self.invoice_month}/{striped_invoice_path}.pdf"
            output_s3_archive_path = f"Invoices/{self.invoice_month}/Archive/{striped_invoice_path} {util.get_iso8601_time()}.pdf"
            s3_bucket.upload_file(invoice_path, output_s3_path)
            s3_bucket.upload_file(invoice_path, output_s3_archive_path)

        # self.name is name of folder storing PDF invoices
        for invoice_filename in os.listdir(self.name):
            _export_s3_group_invoice(invoice_filename)
