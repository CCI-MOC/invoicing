import sys
import datetime
import logging
import os

import pandas
import pyarrow

from process_report.config import config
from process_report import util
from process_report.invoices import (
    lenovo_invoice,
    nonbillable_invoice,
    billable_invoice,
    NERC_total_invoice,
    bu_internal_invoice,
    pi_specific_invoice,
    MOCA_prepaid_invoice,
    prepay_credits_snapshot,
    ocp_test_invoice,
)
from process_report.processors import (
    coldfront_fetch_processor,
    validate_pi_alias_processor,
    add_institution_processor,
    lenovo_processor,
    validate_billable_pi_processor,
    new_pi_credit_processor,
    bu_subsidy_processor,
    prepayment_processor,
    validate_cluster_name_processor,
)

### PI file field names
PI_PI_FIELD = "PI"
PI_FIRST_MONTH = "First Invoice Month"
PI_INITIAL_CREDITS = "Initial Credits"
PI_1ST_USED = "1st Month Used"
PI_2ND_USED = "2nd Month Used"
###


### Invoice field names
INVOICE_DATE_FIELD = "Invoice Month"
PROJECT_FIELD = "Project - Allocation"
PROJECT_ID_FIELD = "Project - Allocation ID"
PI_FIELD = "Manager (PI)"
INVOICE_EMAIL_FIELD = "Invoice Email"
INVOICE_ADDRESS_FIELD = "Invoice Address"
INSTITUTION_FIELD = "Institution"
INSTITUTION_ID_FIELD = "Institution - Specific Code"
SU_HOURS_FIELD = "SU Hours (GBhr or SUhr)"
SU_TYPE_FIELD = "SU Type"
RATE_FIELD = "Rate"
COST_FIELD = "Cost"
CREDIT_FIELD = "Credit"
CREDIT_CODE_FIELD = "Credit Code"
SUBSIDY_FIELD = "Subsidy"
BALANCE_FIELD = "Balance"
###

PI_S3_FILEPATH = "PIs/PI.csv"
ALIAS_S3_FILEPATH = "PIs/alias.csv"
PREPAY_DEBITS_S3_FILEPATH = "Prepay/prepay_debits.csv"


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def get_iso8601_time():
    return datetime.datetime.now().strftime("%Y%m%dT%H%M%SZ")


def validate_required_env_vars(required_env_vars):
    for required_env_var in required_env_vars:
        if required_env_var not in os.environ:
            sys.exit(f"Required environment variable {required_env_var} is not set")


def main():
    """Remove non-billable PIs and projects"""
    required_env_vars = []
    if not config.COLDFRONT_API_FILEPATH:
        required_env_vars.extend(["KEYCLOAK_CLIENT_ID", "KEYCLOAK_CLIENT_SECRET"])
    validate_required_env_vars(required_env_vars)

    invoice_month = config.INVOICE_MONTH

    merged_dataframe = merge_csv(config.get_csv_invoice_filepaths())

    logger.info("Invoice date: " + str(invoice_month))
    logger.info("The following timed-projects will not be billed for this period: ")
    logger.info(config.get_nonbillable_timed_projects())

    ### Preliminary processing
    processed_data = process_merged_dataframe(
        invoice_month,
        merged_dataframe,
        [
            validate_cluster_name_processor.ValidateClusterNameProcessor,
            coldfront_fetch_processor.ColdfrontFetchProcessor,
            validate_pi_alias_processor.ValidatePIAliasProcessor,
            add_institution_processor.AddInstitutionProcessor,
            lenovo_processor.LenovoProcessor,
            validate_billable_pi_processor.ValidateBillablePIsProcessor,
            new_pi_credit_processor.NewPICreditProcessor,
            bu_subsidy_processor.BUSubsidyProcessor,
            prepayment_processor.PrepaymentProcessor,
        ],
    )

    ### Export invoices
    util.process_and_export_invoices(
        invoice_month,
        processed_data,
        [
            lenovo_invoice.LenovoInvoice,
            nonbillable_invoice.NonbillableInvoice,
            billable_invoice.BillableInvoice,
            NERC_total_invoice.NERCTotalInvoice,
            bu_internal_invoice.BUInternalInvoice,
            pi_specific_invoice.PIInvoice,
            MOCA_prepaid_invoice.MOCAPrepaidInvoice,
            prepay_credits_snapshot.PrepayCreditsSnapshot,
            ocp_test_invoice.OcpTestInvoice,
        ],
        config.UPLOAD_TO_S3,
    )


def merge_csv(files):
    """Merge multiple CSV files and return a single pandas dataframe"""
    dataframes = []
    for file in files:
        dataframe = pandas.read_csv(
            file,
            dtype={
                COST_FIELD: pandas.ArrowDtype(pyarrow.decimal128(12, 2)),
                RATE_FIELD: str,
            },
        )
        dataframes.append(dataframe)

    merged_dataframe = pandas.concat(dataframes, ignore_index=True)
    merged_dataframe.reset_index(drop=True, inplace=True)
    return merged_dataframe


def get_invoice_date(dataframe):
    """Returns the invoice date as a pandas timestamp object

    Note that it only checks the first entry because it should
    be the same for every row.
    """
    invoice_date_str = dataframe[INVOICE_DATE_FIELD][0]
    invoice_date = pandas.to_datetime(invoice_date_str, format="%Y-%m")
    return invoice_date


def process_merged_dataframe(
    invoice_month, dataframe: pandas.DataFrame, processors: list
) -> pandas.DataFrame:
    for processor in processors:
        proc_instance = processor(name="", invoice_month=invoice_month, data=dataframe)
        proc_instance.process()
        dataframe = proc_instance.data
    return dataframe


def backup_to_s3_old_pi_file(old_pi_file):
    invoice_bucket = util.get_invoice_bucket()
    invoice_bucket.upload_file(old_pi_file, f"PIs/Archive/PI {get_iso8601_time()}.csv")


def export_billables(dataframe, output_file):
    dataframe.to_csv(output_file, index=False)


if __name__ == "__main__":
    main()
