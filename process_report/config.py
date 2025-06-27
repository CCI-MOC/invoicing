import functools
from datetime import datetime, timedelta
from decimal import Decimal

import pandas
from nerc_rates import load_from_url

from process_report import util


PI_S3_FILEPATH = "PIs/PI.csv"
ALIAS_S3_FILEPATH = "PIs/alias.csv"
PREPAY_DEBITS_S3_FILEPATH = "Prepay/prepay_debits.csv"


rates_info = load_from_url()


def default_invoice_month():
    """
    Start of the invoicing period (YYYY-MM). Defaults to last month if 1st of a month, or this month otherwise.
    """
    d = datetime.today() - timedelta(days=1)
    return d.strftime("%Y-%m")


def fetch_s3_invoices(invoice_month):
    """Fetches usage invoices from S3 given invoice month"""
    s3_invoice_list = list()
    invoice_bucket = util.get_invoice_bucket()
    for obj in invoice_bucket.objects.filter(
        Prefix=f"Invoices/{invoice_month}/Service Invoices/"
    ):
        local_name = obj.key.split("/")[-1]
        s3_invoice_list.append(local_name)
        invoice_bucket.download_file(obj.key, local_name)

    return s3_invoice_list


INVOICE_MONTH = default_invoice_month()
UPLOAD_TO_S3 = True


### Invoice file names
NONBILLABLE_INVOICE_NAME = "nonbillable"
BILLABLE_INVOICE_NAME = "billable"
BU_INTERNAL_INVOICE_NAME = "BU_Internal"
NERC_TOTAL_INVOICE_NAME = "NERC"
LENOVO_INVOICE_NAME = "Lenovo"
OCP_TEST_INVOICE_NAME = "OCP_TEST"
PI_SPECIFIC_FOLDER_NAME = "pi_invoices"

### Input file paths
NONBILLABLE_PIS_FILEPATH = "pi.txt"
NONBILLABLE_PROJECTS_FILEPATH = "project.txt"
NONBILLABLE_TIMED_PROECTS_FILEPATH = "timed_project.txt"

PREPAY_PROJECTS_FILEPATH = "prepaid_projects.csv"
PREPAY_CREDITS_FILEPATH = "prepaid_credits.csv"
PREPAY_CONTACTS_FILEPATH = "prepaid_contacts.csv"

CSV_INVOICE_FILEPATH_LIST = fetch_s3_invoices(INVOICE_MONTH)
PREPAY_DEBITS_FILEPATH = util.fetch_s3(PREPAY_DEBITS_S3_FILEPATH)
OLD_PI_FILEPATH = util.fetch_s3(PI_S3_FILEPATH)
ALIAS_FILEPATH = util.fetch_s3(ALIAS_S3_FILEPATH)
COLDFRONT_API_FILEPATH = ""  # Defaults to being fetched in ColdfrontFetchProcessor


### Miscellaneous config values
NEW_PI_CREDIT_AMOUNT = rates_info.get_value_at("New PI Credit", INVOICE_MONTH, Decimal)
LIMIT_NEW_PI_CREDIT_TO_PARTNERS = rates_info.get_value_at(
    "Limit New PI Credit to MGHPCC Partners", INVOICE_MONTH, bool
)
BU_SUBSIDY_AMOUNT = rates_info.get_value_at("BU Subsidy", INVOICE_MONTH, Decimal)


### Getter functions. Don't change these
@functools.lru_cache
def get_nonbillable_pis():
    with open(NONBILLABLE_PIS_FILEPATH) as file:
        return [line.rstrip() for line in file]


@functools.lru_cache
def get_nonbillable_projects():
    """Returns list of nonbillable projects for current invoice month"""
    with open(NONBILLABLE_PROJECTS_FILEPATH) as file:
        projects = [line.rstrip() for line in file]

    timed_projects_list = get_nonbillable_timed_projects(
        NONBILLABLE_TIMED_PROECTS_FILEPATH, INVOICE_MONTH
    )
    return list(set(projects + timed_projects_list))


@functools.lru_cache
def get_nonbillable_timed_projects(timed_projects_file, invoice_date):
    """Returns list of projects that should be excluded based on dates"""
    dataframe = pandas.read_csv(timed_projects_file)

    # convert to pandas timestamp objects
    dataframe["Start Date"] = pandas.to_datetime(
        dataframe["Start Date"], format="%Y-%m"
    )
    dataframe["End Date"] = pandas.to_datetime(dataframe["End Date"], format="%Y-%m")

    mask = (dataframe["Start Date"] <= invoice_date) & (
        invoice_date <= dataframe["End Date"]
    )
    return dataframe[mask]["Project"].to_list()


@functools.lru_cache
def get_alias_map() -> dict:
    alias_dict = dict()
    with open(ALIAS_FILEPATH) as f:
        for line in f:
            pi_alias_info = line.strip().split(",")
            alias_dict[pi_alias_info[0]] = pi_alias_info[1:]

    return alias_dict


@functools.lru_cache
def get_prepaid_credits_df() -> pandas.DataFrame:
    pandas.read_csv(PREPAY_CREDITS_FILEPATH)


@functools.lru_cache
def get_prepaid_projects_df() -> pandas.DataFrame:
    pandas.read_csv(PREPAY_PROJECTS_FILEPATH)


@functools.lru_cache
def get_prepaid_contacts_df() -> pandas.DataFrame:
    pandas.read_csv(PREPAY_CONTACTS_FILEPATH)
