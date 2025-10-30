import os
import sys
import functools
import logging
import json
from dataclasses import dataclass, field

import requests
import pandas

from process_report.loader import loader
from process_report.settings import invoice_settings
from process_report.invoices import invoice
from process_report.processors import processor, validate_billable_pi_processor

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


CF_ATTR_ALLOCATED_PROJECT_NAME = "Allocated Project Name"
CF_ATTR_ALLOCATED_PROJECT_ID = "Allocated Project ID"
CF_ATTR_INSTITUTION_SPECIFIC_CODE = "Institution-Specific Code"


@dataclass
class ColdfrontFetchProcessor(processor.Processor):
    nonbillable_projects: pandas.DataFrame = field(
        default_factory=loader.get_nonbillable_projects
    )
    coldfront_data_filepath: str = invoice_settings.coldfront_api_filepath

    @functools.cached_property
    def coldfront_client(self):
        keycloak_url = os.environ.get("KEYCLOAK_URL", "https://keycloak.mss.mghpcc.org")

        # Authenticate with Keycloak
        token_url = f"{keycloak_url}/auth/realms/mss/protocol/openid-connect/token"
        r = requests.post(
            token_url,
            data={"grant_type": "client_credentials"},
            auth=requests.auth.HTTPBasicAuth(
                os.environ["KEYCLOAK_CLIENT_ID"],
                os.environ["KEYCLOAK_CLIENT_SECRET"],
            ),
        )
        try:
            r.raise_for_status()
        except requests.HTTPError:
            sys.exit(f"Keycloak authentication failed:\n{r.status_code} {r.text}")

        client_token = r.json()["access_token"]

        session = requests.session()
        headers = {
            "Authorization": f"Bearer {client_token}",
            "Content-Type": "application/json",
        }
        session.headers.update(headers)
        return session

    def _get_project_name_list(self) -> list[str]:
        """Returns list of billable project IDs"""
        project_mask = validate_billable_pi_processor.find_billable_projects(
            self.data, self.nonbillable_projects
        )
        return self.data[project_mask][invoice.PROJECT_FIELD].unique().tolist()

    def _fetch_coldfront_allocation_api(self):
        coldfront_api_url = os.environ.get(
            "COLDFRONT_URL", "https://coldfront.mss.mghpcc.org/api/allocations"
        )
        r = self.coldfront_client.get(f"{coldfront_api_url}?all=true")

        return r.json()

    def _get_coldfront_api_data(self):
        if self.coldfront_data_filepath:
            logger.info(
                f"Using Coldfront data from {self.coldfront_data_filepath} instead of API"
            )
            with open(self.coldfront_data_filepath, "r") as f:
                return json.load(f)
        else:
            return self._fetch_coldfront_allocation_api()

    def _get_allocation_data(self, coldfront_api_data):
        """Returns a mapping of project IDs to a dict of project name, PI name, and institution code."""
        allocation_data = {}
        for project_dict in coldfront_api_data:
            try:
                # Allow allocation to not have institute code
                project_id = project_dict["attributes"][CF_ATTR_ALLOCATED_PROJECT_ID]
                project_name = project_dict["attributes"][
                    CF_ATTR_ALLOCATED_PROJECT_NAME
                ]
                pi_name = project_dict["project"]["pi"]
                institute_code = project_dict["attributes"].get(
                    CF_ATTR_INSTITUTION_SPECIFIC_CODE, "N/A"
                )
                allocation_data[project_id] = {
                    invoice.PROJECT_FIELD: project_name,
                    invoice.PI_FIELD: pi_name,
                    invoice.INSTITUTION_ID_FIELD: institute_code,
                }
            except KeyError:
                continue

        return allocation_data

    def _validate_allocation_data(self, allocation_data):
        # TODO (Quan): If two Coldfront projects have the same name
        # (maybe because they exist on different clusters),
        # the `allocation_data` dict will only accurately store info for one of the projects
        # Would this be a concern?
        allocation_project_names = {
            data[invoice.PROJECT_FIELD] for data in allocation_data.values()
        }
        missing_projects = set(self._get_project_name_list()) - allocation_project_names
        missing_projects = list(missing_projects)
        missing_projects.sort()  # Ensures order for testing purposes
        if missing_projects:
            raise ValueError(
                f"Projects {missing_projects} not found in Coldfront and are billable! Please check the project names"
            )

    def _apply_allocation_data(self, allocation_data):
        for project_id, data in allocation_data.items():
            mask = self.data[invoice.PROJECT_ID_FIELD] == project_id
            self.data.loc[mask, invoice.PROJECT_FIELD] = data[invoice.PROJECT_FIELD]
            self.data.loc[mask, invoice.PI_FIELD] = data[invoice.PI_FIELD]
            self.data.loc[mask, invoice.INSTITUTION_ID_FIELD] = data[
                invoice.INSTITUTION_ID_FIELD
            ]

    def _process(self):
        api_data = self._get_coldfront_api_data()
        allocation_data = self._get_allocation_data(api_data)

        #
        self._apply_allocation_data(allocation_data)
        self._validate_allocation_data(allocation_data)
