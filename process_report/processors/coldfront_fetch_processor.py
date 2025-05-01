import os
import sys
import functools
import logging
from dataclasses import dataclass

import requests

from process_report.invoices import invoice
from process_report.processors import processor

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


CF_ATTR_ALLOCATED_PROJECT_NAME = "Allocated Project Name"
CF_ATTR_ALLOCATED_PROJECT_ID = "Allocated Project ID"
CF_ATTR_INSTITUTION_SPECIFIC_CODE = "Institution-Specific Code"

@dataclass
class ColdfrontFetchProcessor(processor.Processor):
    nonbillable_projects: list[str]

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

    def _get_project_list(self):
        return self.data[invoice.PROJECT_FIELD].unique()

    def _fetch_coldfront_allocation_api(self, allocation_list):
        coldfront_api_url = os.environ.get(
            "COLDFRONT_URL", "https://coldfront.mss.mghpcc.org/api/allocations"
        )
        r = self.coldfront_client.get(f"{coldfront_api_url}?all=true")
        return r.json()

    def _get_allocation_data(self, coldfront_api_data):
        allocation_data = {}
        for project, project_dict in coldfront_api_data.items():
            project_id = project_dict["attributes"][CF_ATTR_ALLOCATED_PROJECT_ID]
            if project_id in self.data[invoice.PROJECT_ID_FIELD]:
                allocation_data[project_id] = {
                    invoice.PI_FIELD: project_dict["project"]["pi"],
                    invoice.PROJECT_FIELD: project_dict["attributes"][
                        CF_ATTR_ALLOCATED_PROJECT_NAME],
                    invoice.INSTITUTION_ID_FIELD: project_dict["attributes"][
                        CF_ATTR_INSTITUTION_SPECIFIC_CODE
                    ],
                }
        return allocation_data

    def _validate_allocation_data(self, allocation_data):
        missing_projects = (
            set(self._get_project_list())
            - set(allocation_data.keys())
            - set(self.nonbillable_projects)
        )
        missing_projects = list(missing_projects)
        missing_projects.sort()  # Ensures order for testing purposes
        if missing_projects:
            logger.warning(
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
        project_allocations_list = self._get_project_list()
        api_data = self._fetch_coldfront_allocation_api(project_allocations_list)
        allocation_data = self._get_allocation_data(api_data)
        # self._validate_allocation_data(allocation_data)
        self._apply_allocation_data(allocation_data)
