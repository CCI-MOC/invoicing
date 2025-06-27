import functools
from datetime import datetime, timedelta
from decimal import Decimal

import pandas
import pydantic
from nerc_rates import load_from_url

from process_report import util


@functools.lru_cache
def get_rates_info():
    return load_from_url()


def default_invoice_month():
    """
    Start of the invoicing period (YYYY-MM). Defaults to last month if 1st of a month, or this month otherwise.
    """
    d = datetime.today() - timedelta(days=1)
    return d.strftime("%Y-%m")


class Config(pydantic.BaseModel):
    INVOICE_MONTH: str = pydantic.Field(default=default_invoice_month())
    UPLOAD_TO_S3: bool = True

    # S3 file paths
    PI_S3_FILEPATH: str = "PIs/PI.csv"
    ALIAS_S3_FILEPATH: str = "PIs/alias.csv"
    PREPAY_DEBITS_S3_FILEPATH: str = "Prepay/prepay_debits.csv"

    # Invoice file names
    NONBILLABLE_INVOICE_NAME: str = "nonbillable"
    BILLABLE_INVOICE_NAME: str = "billable"
    BU_INTERNAL_INVOICE_NAME: str = "BU_Internal"
    NERC_TOTAL_INVOICE_NAME: str = "NERC"
    LENOVO_INVOICE_NAME: str = "Lenovo"
    OCP_TEST_INVOICE_NAME: str = "OCP_TEST"
    PI_SPECIFIC_FOLDER_NAME: str = "pi_invoices"

    # Input file paths
    NONBILLABLE_PIS_FILEPATH: pydantic.FilePath = "pi.txt"
    NONBILLABLE_PROJECTS_FILEPATH: pydantic.FilePath = "projects.txt"
    NONBILLABLE_TIMED_PROECTS_FILEPATH: pydantic.FilePath = "timed_projects.txt"

    PREPAY_PROJECTS_FILEPATH: pydantic.FilePath = "prepaid_projects.csv"
    PREPAY_CREDITS_FILEPATH: pydantic.FilePath = "prepaid_credits.csv"
    PREPAY_CONTACTS_FILEPATH: pydantic.FilePath = "prepaid_contacts.csv"

    # Defaults to being fetched from S3
    CSV_INVOICE_FILEPATH_LIST: list[pydantic.FilePath] | None = None
    PREPAY_DEBITS_FILEPATH: pydantic.FilePath | None = None
    OLD_PI_FILEPATH: pydantic.FilePath | None = None
    ALIAS_FILEPATH: pydantic.FilePath | None = None

    # Defaults to being fetched in ColdfrontFetchProcessor
    COLDFRONT_API_FILEPATH: pydantic.FilePath | None = None

    # Defaults to being fetched in `nerc_rates`
    NEW_PI_CREDIT_AMOUNT: Decimal | None = None
    LIMIT_NEW_PI_CREDIT_TO_PARTNERS: bool | None = None
    BU_SUBSIDY_AMOUNT: Decimal | None = None
    LENOVO_CHARGE_INFO: dict[str, Decimal] | None = None

    model_config = pydantic.ConfigDict(extra="forbid")

    @staticmethod
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

    def get_csv_invoice_filepaths(self) -> list[pydantic.FilePath]:
        if not self.CSV_INVOICE_FILEPATH_LIST:
            self.CSV_INVOICE_FILEPATH_LIST = self.fetch_s3_invoices(self.INVOICE_MONTH)
        return self.CSV_INVOICE_FILEPATH_LIST

    def get_prepay_debits_filepath(self) -> pydantic.FilePath:
        if not self.PREPAY_DEBITS_FILEPATH:
            self.PREPAY_DEBITS_FILEPATH = util.fetch_s3(self.PREPAY_DEBITS_S3_FILEPATH)
        return self.PREPAY_DEBITS_FILEPATH

    def get_old_pi_filepath(self) -> pydantic.FilePath:
        if not self.OLD_PI_FILEPATH:
            self.OLD_PI_FILEPATH = util.fetch_s3(self.PI_S3_FILEPATH)
        return self.OLD_PI_FILEPATH

    def get_alias_filepath(self) -> pydantic.FilePath:
        if not self.ALIAS_FILEPATH:
            self.ALIAS_FILEPATH = util.fetch_s3(self.ALIAS_S3_FILEPATH)
        return self.ALIAS_FILEPATH

    def get_new_pi_credit_amount(self) -> Decimal:
        if not self.NEW_PI_CREDIT_AMOUNT:
            self.NEW_PI_CREDIT_AMOUNT = get_rates_info().get_value_at(
                "New PI Credit", self.INVOICE_MONTH, Decimal
            )
        return self.NEW_PI_CREDIT_AMOUNT

    def get_limit_new_pi_credit_to_partners(self) -> bool:
        if not self.LIMIT_NEW_PI_CREDIT_TO_PARTNERS:
            self.LIMIT_NEW_PI_CREDIT_TO_PARTNERS = get_rates_info().get_value_at(
                "Limit New PI Credit to MGHPCC Partners", self.INVOICE_MONTH, bool
            )
        return self.LIMIT_NEW_PI_CREDIT_TO_PARTNERS

    def get_bu_subsidy_amount(self) -> Decimal:
        if not self.BU_SUBSIDY_AMOUNT:
            self.BU_SUBSIDY_AMOUNT = get_rates_info().get_value_at(
                "BU Subsidy", self.INVOICE_MONTH, Decimal
            )
        return self.BU_SUBSIDY_AMOUNT

    def get_lenovo_su_charge_info(self) -> dict[str, Decimal]:
        if not self.LENOVO_CHARGE_INFO:
            self.LENOVO_CHARGE_INFO = {}
            for su_name in ["GPUA100SXM4", "GPUH100"]:
                self.LENOVO_CHARGE_INFO[su_name] = get_rates_info().get_value_at(
                    f"Lenovo {su_name} Charge", self.INVOICE_MONTH, Decimal
                )
        return self.LENOVO_CHARGE_INFO

    # Some invoices/processors want files loaded in certain ways
    def get_nonbillable_pis(self) -> list[str]:
        with open(self.NONBILLABLE_PIS_FILEPATH) as file:
            return [line.rstrip() for line in file]

    def get_nonbillable_projects(self) -> list[str]:
        """Returns list of nonbillable projects for current invoice month"""
        with open(self.NONBILLABLE_PROJECTS_FILEPATH) as file:
            projects = [line.rstrip() for line in file]

        timed_projects_list = self.get_nonbillable_timed_projects()
        return list(set(projects + timed_projects_list))

    def get_nonbillable_timed_projects(self) -> list[str]:
        """Returns list of projects that should be excluded based on dates"""
        dataframe = pandas.read_csv(self.NONBILLABLE_TIMED_PROECTS_FILEPATH)

        # convert to pandas timestamp objects
        dataframe["Start Date"] = pandas.to_datetime(
            dataframe["Start Date"], format="%Y-%m"
        )
        dataframe["End Date"] = pandas.to_datetime(
            dataframe["End Date"], format="%Y-%m"
        )

        mask = (dataframe["Start Date"] <= self.INVOICE_MONTH) & (
            self.INVOICE_MONTH <= dataframe["End Date"]
        )
        return dataframe[mask]["Project"].to_list()

    def get_alias_map(self) -> dict:
        alias_dict = dict()
        with open(self.get_alias_filepath()) as f:
            for line in f:
                pi_alias_info = line.strip().split(",")
                alias_dict[pi_alias_info[0]] = pi_alias_info[1:]

        return alias_dict

    def get_prepaid_credits_df(self) -> pandas.DataFrame:
        return pandas.read_csv(self.PREPAY_CREDITS_FILEPATH)

    def get_prepaid_projects_df(self) -> pandas.DataFrame:
        return pandas.read_csv(self.PREPAY_PROJECTS_FILEPATH)

    def get_prepaid_contacts_df(self) -> pandas.DataFrame:
        return pandas.read_csv(self.PREPAY_CONTACTS_FILEPATH)


# Custom configurations goes here
CONFIG_DICT = {}
config = Config.model_validate(CONFIG_DICT)
