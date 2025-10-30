from dataclasses import dataclass, field
import logging

import pandas

from process_report.loader import loader
from process_report.invoices import invoice
from process_report.processors import processor

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


NONBILLABLE_CLUSTERS = ["ocp-test"]


def find_billable_projects(
    data: pandas.DataFrame, nonbillable_projects: pandas.DataFrame
) -> pandas.Series:
    """
    Takes as input:
    - `data`: DataFrame containing invoice data with project and cluster columns
    - `nonbillable_projects`: DataFrame containing nonbillable projects with project and cluster columns
    Returns a boolean series indicating whether each project in `data` is billable

    Several comparisons are made:
    - On cluster-agnostic nonbillable projects
    - On cluster-specific nonbillable projects
    - On nonbillable clusters, currently only `ocp-test`
    Project names are compared in a case-insensitive manner.

    There is a convoluted reason why the `Project - Allocation` column is checked:
    Input invoices to this pipeline are expected to have the `Project - Allocation`
    and `Project - Allocation ID` columns both populated by the project ID.
    `nonbillable_projects` usually identify projects by names, which are more
    human-readable. However, we found it acceptable to use IDs for non-Coldfront projects
    `ColdfrontFetchProcessor` attempts to populate `Project - Allocation` with project
    names, then checks if all non-Coldfront projects are nonbillable
    This check works because we allow `nonbillable_projects` to contain project IDs for non-Coldfront projects.

    Ultimately, it is important to note that `Project - Allocation` may contain the project name or ID.
    """

    def _str_to_lowercase(data):
        return data.lower()

    data_lowercase = data.copy()
    data_lowercase[invoice.PROJECT_FIELD] = data_lowercase[invoice.PROJECT_FIELD].apply(
        _str_to_lowercase
    )
    nonbillable_projects_lowercase = nonbillable_projects.copy()
    nonbillable_projects_lowercase[invoice.NONBILLABLE_PROJECT_NAME] = (
        nonbillable_projects_lowercase[invoice.NONBILLABLE_PROJECT_NAME].apply(
            _str_to_lowercase
        )
    )
    cluster_agnostic_projects = (
        nonbillable_projects_lowercase[
            nonbillable_projects_lowercase[invoice.NONBILLABLE_CLUSTER_NAME].isna()
        ][invoice.NONBILLABLE_PROJECT_NAME]
        .unique()
        .tolist()
    )

    # Use left join and filter on `source` column to find billable projects
    # https://pandas.pydata.org/docs/reference/api/pandas.merge.html
    merged_data = pandas.merge(
        data_lowercase,
        nonbillable_projects_lowercase,
        how="left",
        left_on=[invoice.PROJECT_FIELD, invoice.CLUSTER_NAME_FIELD],
        right_on=[invoice.NONBILLABLE_PROJECT_NAME, invoice.NONBILLABLE_CLUSTER_NAME],
        indicator="source",
    )

    cluster_agnostic_mask = ~merged_data[invoice.PROJECT_FIELD].isin(
        cluster_agnostic_projects
    )
    cluster_specific_mask = ~merged_data["source"].eq("both")
    nonbillable_cluster_mask = ~merged_data[invoice.CLUSTER_NAME_FIELD].isin(
        NONBILLABLE_CLUSTERS
    )
    return cluster_agnostic_mask & cluster_specific_mask & nonbillable_cluster_mask


@dataclass
class ValidateBillablePIsProcessor(processor.Processor):
    """
    This processor validates the billable PIs and projects in the data,
    and determines if a project is billable or not.

    Every project belonging to ocp-test is nonbillable.
    """

    nonbillable_pis: list[str] = field(default_factory=loader.get_nonbillable_pis)
    nonbillable_projects: pandas.DataFrame = field(
        default_factory=loader.get_nonbillable_projects
    )

    @staticmethod
    def _validate_pi_names(data: pandas.DataFrame):
        invalid_pi_projects = data[pandas.isna(data[invoice.PI_FIELD])]
        for i, row in invalid_pi_projects.iterrows():
            if row[invoice.IS_BILLABLE_FIELD]:
                logger.warning(
                    f"Billable project {row[invoice.PROJECT_FIELD]} has empty PI field"
                )
        return pandas.isna(data[invoice.PI_FIELD])

    @staticmethod
    def _get_billables(
        data: pandas.DataFrame,
        nonbillable_pis: list[str],
        nonbillable_projects: pandas.DataFrame,
    ):
        pi_mask = ~data[invoice.PI_FIELD].isin(nonbillable_pis)
        project_mask = find_billable_projects(data, nonbillable_projects)

        return pi_mask & project_mask

    def _process(self):
        self.data[invoice.IS_BILLABLE_FIELD] = self._get_billables(
            self.data, self.nonbillable_pis, self.nonbillable_projects
        )
        self.data[invoice.MISSING_PI_FIELD] = self._validate_pi_names(self.data)
