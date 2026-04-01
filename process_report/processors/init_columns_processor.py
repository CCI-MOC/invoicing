import logging
from decimal import Decimal
from dataclasses import dataclass

from process_report.invoices import invoice
from process_report.processors import discount_processor


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

INVOICE_FIELD_LIST = [
    invoice.INVOICE_DATE_FIELD,
    invoice.PROJECT_FIELD,
    invoice.PROJECT_ID_FIELD,
    invoice.PI_FIELD,
    invoice.INVOICE_EMAIL_FIELD,
    invoice.INVOICE_ADDRESS_FIELD,
    invoice.INSTITUTION_FIELD,
    invoice.INSTITUTION_ID_FIELD,
    invoice.GROUP_NAME_FIELD,
    invoice.GROUP_INSTITUTION_FIELD,
    invoice.GROUP_BALANCE_FIELD,
    invoice.GROUP_BALANCE_USED_FIELD,
    invoice.SU_HOURS_FIELD,
    invoice.SU_TYPE_FIELD,
    invoice.SU_CHARGE_FIELD,
    invoice.LENOVO_CHARGE_FIELD,
    invoice.RATE_FIELD,
    invoice.COST_FIELD,
    invoice.CREDIT_FIELD,
    invoice.CREDIT_CODE_FIELD,
    invoice.SUBSIDY_FIELD,
    invoice.BALANCE_FIELD,
    # Internally used fields
    invoice.IS_BILLABLE_FIELD,
    invoice.MISSING_PI_FIELD,
    invoice.PI_BALANCE_FIELD,
    invoice.PROJECT_NAME_FIELD,
    invoice.GROUP_MANAGED_FIELD,
    invoice.CLUSTER_NAME_FIELD,
    invoice.IS_COURSE_FIELD,
]

# Fields without defaults have None as default value
FIELD_DEFAULT_MAPPING = {
    invoice.BALANCE_FIELD: invoice.COST_FIELD,
    invoice.PI_BALANCE_FIELD: invoice.COST_FIELD,
    invoice.SUBSIDY_FIELD: Decimal(0),
    invoice.IS_COURSE_FIELD: False,  # All rows assumed to be non-course, unless marked otherwise by ColdfrontFetchProcessor
}


@dataclass
class PISUCreditProcessor(discount_processor.DiscountProcessor):
    """
    Initializes all columns that will be used in later Processors, with appropriate types and default values.

    Fields without defaults specified in FIELD_DEFAULT_MAPPING will have None as default value
    Pre-existing columns in input dataframe will only be casted, not overwritten
    """

    def _process(self):
        for field in INVOICE_FIELD_LIST:
            if field not in self.data.columns:
                default_value = FIELD_DEFAULT_MAPPING.get(field, None)

                # If default value is name of another column, copy values from that column and cast to correct type
                if default_value in INVOICE_FIELD_LIST:
                    default_value = self.data[default_value]

                self.data[field] = default_value
