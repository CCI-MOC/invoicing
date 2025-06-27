from dataclasses import dataclass

from process_report.invoices import invoice


@dataclass
class BillableInvoice(invoice.Invoice):
    """
    This invoice operates on data processed by these Processors:
    - ValidateBillablePIsProcessor
    - NewPICreditProcessor
    """

    name: str = "billable"
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

    def _prepare_export(self):
        self.export_data = self.data[
            self.data[invoice.IS_BILLABLE_FIELD] & ~self.data[invoice.MISSING_PI_FIELD]
        ]
