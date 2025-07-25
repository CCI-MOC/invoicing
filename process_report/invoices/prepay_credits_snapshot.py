from dataclasses import dataclass, field

import pandas

from process_report.config import config
from process_report import util
from process_report.invoices import invoice


@dataclass
class PrepayCreditsSnapshot(invoice.Invoice):
    name: str = ""
    prepay_credits: pandas.DataFrame = field(
        default_factory=config.get_prepaid_credits_df
    )
    prepay_contacts: pandas.DataFrame = field(
        default_factory=config.get_prepaid_contacts_df
    )
    export_columns_list = [
        invoice.PREPAY_MONTH_FIELD,
        invoice.PREPAY_GROUP_NAME_FIELD,
        invoice.PREPAY_CREDIT_FIELD,
    ]

    @property
    def output_path(self):
        return f"NERC_Prepaid_Group-Credits-{self.invoice_month}.csv"

    @property
    def output_s3_key(self):
        return f"Invoices/{self.invoice_month}/NERC_Prepaid_Group-Credits-{self.invoice_month}.csv"

    @property
    def output_s3_archive_key(self):
        return f"Invoices/{self.invoice_month}/Archive/NERC_Prepaid_Group-Credits-{self.invoice_month} {util.get_iso8601_time()}.csv"

    def _get_prepay_credits_snapshot(self):
        managed_groups_list = self.prepay_contacts.loc[
            self.prepay_contacts[invoice.PREPAY_MANAGED_FIELD] == "Yes",
            invoice.PREPAY_GROUP_NAME_FIELD,
        ]

        credits_mask = (
            self.prepay_credits[invoice.PREPAY_MONTH_FIELD] == self.invoice_month
        ) & (
            self.prepay_credits[invoice.PREPAY_GROUP_NAME_FIELD].isin(
                managed_groups_list
            )
        )
        return self.prepay_credits[credits_mask]

    def _prepare(self):
        self.export_data = self._get_prepay_credits_snapshot()
