from decimal import Decimal
import functools
import os
import yaml

import pandas
from nerc_rates import load_from_url

from process_report import util
from process_report.settings import invoice_settings
from process_report.invoices import invoice


@functools.lru_cache
def get_rates_info():
    return load_from_url()


class Loader:
    @functools.lru_cache
    def get_csv_invoice_filepath_list(self) -> list[str]:
        """Fetch invoice CSV files from S3 if fetch_from_s3 is True. Returns local paths of files."""
        csv_invoice_filepath_list = []
        if invoice_settings.fetch_from_s3:
            s3_bucket = util.get_invoice_bucket()

            for obj in s3_bucket.objects.filter(
                Prefix=invoice_settings.invoice_path_template.format(
                    invoice_month=invoice_settings.invoice_month
                )
            ):
                local_name = obj.key.split("/")[-1]
                csv_invoice_filepath_list.append(local_name)
                s3_bucket.download_file(obj.key, local_name)
        else:
            invoice_dir_path = invoice_settings.invoice_path_template.format(
                invoice_month=invoice_settings.invoice_month
            )
            for invoice in os.listdir(invoice_dir_path):
                invoice_absolute_path = os.path.join(invoice_dir_path, invoice)
                csv_invoice_filepath_list.append(invoice_absolute_path)

        return csv_invoice_filepath_list

    @functools.lru_cache
    def get_remote_filepath(self, remote_filepath: str) -> str:
        """Fetch a file from S3 if fetch_from_s3 is True. Returns local path of file."""
        if invoice_settings.fetch_from_s3:
            return util.fetch_s3(remote_filepath)
        return remote_filepath

    @functools.lru_cache
    def get_new_pi_credit_amount(self) -> Decimal:
        return invoice_settings.new_pi_credit_amount or get_rates_info().get_value_at(
            "New PI Credit", invoice_settings.invoice_month, Decimal
        )

    @functools.lru_cache
    def get_limit_new_pi_credit_to_partners(self) -> bool:
        return (
            invoice_settings.limit_new_pi_credit_to_partners
            or get_rates_info().get_value_at(
                "Limit New PI Credit to MGHPCC Partners",
                invoice_settings.invoice_month,
                bool,
            )
        )

    @functools.lru_cache
    def get_bu_subsidy_amount(self) -> Decimal:
        return invoice_settings.bu_subsidy_amount or get_rates_info().get_value_at(
            "BU Subsidy", invoice_settings.invoice_month, Decimal
        )

    @functools.lru_cache
    def get_lenovo_su_charge_info(self) -> dict[str, Decimal]:
        if invoice_settings.lenovo_charge_info:
            return invoice_settings.lenovo_charge_info

        lenovo_charge_info = {}
        for su_name in ["GPUA100SXM4", "GPUH100"]:
            lenovo_charge_info[su_name] = get_rates_info().get_value_at(
                f"Lenovo {su_name} Charge", invoice_settings.invoice_month, Decimal
            )
        return lenovo_charge_info

    @functools.lru_cache
    def get_alias_map(self) -> dict:
        alias_dict = dict()
        with open(
            self.get_remote_filepath(invoice_settings.alias_remote_filepath)
        ) as f:
            for line in f:
                pi_alias_info = line.strip().split(",")
                alias_dict[pi_alias_info[0]] = pi_alias_info[1:]

        return alias_dict

    @functools.lru_cache
    def load_dataframe(self, filepath: str) -> pandas.DataFrame:
        return pandas.read_csv(filepath)

    def get_nonbillable_pis(self) -> list[str]:
        with open(invoice_settings.nonbillable_pis_filepath) as file:
            return [line.rstrip() for line in file]

    @functools.lru_cache
    def get_nonbillable_projects(self) -> pandas.DataFrame:
        """
        Returns dataframe of nonbillable projects for current invoice month
        The dataframe has 3 columns: Project Name, Cluster, Is Timed
        1. Project Name: Name of the nonbillable project
        2. Cluster: Name of the cluster for which the project is nonbillable, or None meaning all clusters
        3. Is Timed: Boolean indicating if the nonbillable status is time-bound
        """

        def _check_time_range(timed_object) -> bool:
            # Leveraging inherent lexicographical order of YYYY-MM strings
            return (
                timed_object["start"] <= invoice_settings.invoice_month
                and invoice_settings.invoice_month < timed_object["end"]
            )

        project_list = []
        with open(invoice_settings.nonbillable_projects_filepath) as file:
            projects_dict = yaml.safe_load(file)

        for project in projects_dict:
            project_name = project["name"]
            cluster_list = project.get("clusters")

            if project.get("start"):
                if _check_time_range(project):
                    if cluster_list:
                        for cluster in cluster_list:
                            project_list.append((project_name, cluster["name"], True))
                    else:
                        project_list.append((project_name, None, True))
            elif cluster_list:
                for cluster in cluster_list:
                    cluster_start_time = cluster.get("start")
                    if cluster_start_time and _check_time_range(cluster):
                        project_list.append((project_name, cluster["name"], True))
                    elif not cluster_start_time:
                        project_list.append((project_name, cluster["name"], False))
            else:
                project_list.append((project_name, None, False))

        return pandas.DataFrame(
            project_list,
            columns=[
                invoice.NONBILLABLE_PROJECT_NAME,
                invoice.NONBILLABLE_CLUSTER_NAME,
                invoice.NONBILLABLE_IS_TIMED,
            ],
        )

    def get_nonbillable_timed_projects(self) -> list[str]:
        """Returns list of projects that should be excluded based on dates"""
        # TODO (Quan): Do we also want to specify the clusters for these timed projects?
        nonbilable_projects = self.get_nonbillable_projects()
        return (
            nonbilable_projects[nonbilable_projects[invoice.NONBILLABLE_IS_TIMED]][
                invoice.NONBILLABLE_PROJECT_NAME
            ]
            .unique()
            .tolist()
        )


loader = Loader()
