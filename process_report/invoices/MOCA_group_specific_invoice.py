import os
from dataclasses import dataclass
import tempfile

import pandas

from process_report.invoices import invoice, pdf_invoice


@dataclass
class MOCAGroupInvoice(pdf_invoice.PDFInvoice):
    CREDIT_COLUMN_COPY_LIST = [
        invoice.INVOICE_DATE_FIELD,
        invoice.INVOICE_EMAIL_FIELD,
        invoice.GROUP_NAME_FIELD,
        invoice.GROUP_INSTITUTION_FIELD,
    ]
    TOTAL_COLUMN_LIST = [
        invoice.COST_FIELD,
        invoice.GROUP_BALANCE_USED_FIELD,
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

    prepay_credits: pandas.DataFrame

    def _prepare(self):
        self.export_data = self.data[
            self.data[invoice.IS_BILLABLE_FIELD] & ~self.data[invoice.MISSING_PI_FIELD]
        ]
        self.export_data = self.export_data[
            ~self.export_data[invoice.GROUP_NAME_FIELD].isna()
        ]
        self.group_list = self.export_data[invoice.GROUP_NAME_FIELD].unique()

    def _get_group_dataframe(self, data, group):
        group_projects = (
            data[data[invoice.GROUP_NAME_FIELD] == group].copy().reset_index(drop=True)
        )

        # Add row for each prepay credit for the group in the invoice month
        group_credit_mask = (
            self.prepay_credits[invoice.PREPAY_MONTH_FIELD] == self.invoice_month
        ) & (self.prepay_credits[invoice.PREPAY_GROUP_NAME_FIELD] == group)
        group_credit_info = self.prepay_credits[group_credit_mask]
        for _, credit_info in group_credit_info.iterrows():
            group_credit = credit_info[invoice.PREPAY_CREDIT_FIELD]
            group_projects.loc[len(group_projects)] = None

            # In this "credit row", certain values should be
            # the same for every columns (i.e Invoice Month, Group Name, etc.)
            for column_name in self.CREDIT_COLUMN_COPY_LIST:
                if column_name in group_projects.columns:
                    group_projects.loc[group_projects.index[-1], column_name] = (
                        group_projects.loc[0, column_name]
                    )

            # Group is billed for the credit amount
            group_projects.loc[
                group_projects.index[-1], [invoice.COST_FIELD, invoice.BALANCE_FIELD]
            ] = [group_credit] * 2

        # Add sum row
        column_sums = []
        sum_columns_list = []
        for column_name in self.TOTAL_COLUMN_LIST:
            if column_name in group_projects.columns:
                column_sums.append(group_projects[column_name].sum())
                sum_columns_list.append(column_name)
        group_projects.loc[len(group_projects)] = (
            None  # Adds a new row to end of dataframe initialized with None
        )
        group_projects.loc[group_projects.index[-1], invoice.INVOICE_DATE_FIELD] = (
            "Total"
        )
        group_projects.loc[group_projects.index[-1], sum_columns_list] = column_sums

        # Add dollar signs
        for column_name in self.DOLLAR_COLUMN_LIST:
            if column_name in group_projects.columns:
                group_projects[column_name] = group_projects[column_name].apply(
                    lambda data: data if pandas.isna(data) else f"${data}"
                )

        group_projects.fillna("", inplace=True)

        return group_projects

    def export(self):
        self._filter_columns()

        if not os.path.exists(self.name):
            os.mkdir(self.name)

        for group in self.group_list:
            group_dataframe = self._get_group_dataframe(self.export_data, group)
            group_instituition = group_dataframe[invoice.GROUP_INSTITUTION_FIELD].iat[0]
            group_contact_email = group_dataframe[invoice.INVOICE_EMAIL_FIELD].iat[0]
            group_invoice_path = f"{self.name}/{group_instituition}_{group_contact_email}_{self.invoice_month}.pdf"

            with tempfile.NamedTemporaryFile(mode="w", suffix=".html") as temp_fd:
                self._create_html_invoice(temp_fd, group_dataframe, "pi_invoice.html")
                self._create_pdf_invoice(temp_fd.name, group_invoice_path)
