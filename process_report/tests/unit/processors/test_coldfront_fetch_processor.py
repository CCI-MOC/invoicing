from unittest import TestCase, mock
import pandas

from process_report.tests import util as test_utils


class TestColdfrontFetchProcessor(TestCase):
    def _get_test_invoice(
        self,
        allocation_project_id,
        allocation_project_name=None,
        pi=None,
        institute_code=None,
    ):
        if not pi:
            pi = [""] * len(allocation_project_id)

        if not institute_code:
            institute_code = [""] * len(allocation_project_id)

        if not allocation_project_name:
            allocation_project_name = allocation_project_id.copy()

        return pandas.DataFrame(
            {
                "Manager (PI)": pi,
                "Project - Allocation ID": allocation_project_id,
                "Project - Allocation": allocation_project_name,
                "Institution - Specific Code": institute_code,
            }
        )

    def _get_mock_allocation_data(self, project_id_list, pi_list, institute_code_list):
        mock_data = {}
        for i, project in enumerate(project_id_list):
            mock_data[project] = {}
            mock_data[project]["project"] = {"pi": pi_list[i]}
            mock_data[project]["attributes"] = {
                "Allocated Project ID": project,
                "Allocated Project Name": f"{project}-name",
                "Institution-Specific Code": institute_code_list[i]
            }

        return mock_data

    @mock.patch(
        "process_report.processors.coldfront_fetch_processor.ColdfrontFetchProcessor._fetch_coldfront_allocation_api",
    )
    def test_coldfront_fetch(self, mock_get_allocation_data):
        mock_get_allocation_data.return_value = self._get_mock_allocation_data(
            ["P1", "P2", "P3", "P4"],
            ["PI1", "PI1", "", "PI12"],
            ["IC1", "", "", "IC2"],
        )
        test_invoice = self._get_test_invoice(["P1", "P1", "P2", "P3", "P4"])
        answer_invoice = self._get_test_invoice(
            ["P1", "P1", "P2", "P3", "P4"],
            allocation_project_name=["P1-name", "P1-name", "P2-name", "P3-name", "P4-name"],
            pi=["PI1", "PI1", "PI1", "", "PI12"],
            institute_code=["IC1", "IC1", "", "", "IC2"],
        )
        test_coldfront_fetch_proc = test_utils.new_coldfront_fetch_processor(
            data=test_invoice
        )
        test_coldfront_fetch_proc.process()
        output_invoice = test_coldfront_fetch_proc.data
        self.assertTrue(output_invoice.equals(answer_invoice))

    @mock.patch(
        "process_report.processors.coldfront_fetch_processor.ColdfrontFetchProcessor._fetch_coldfront_allocation_api",
    )
    def test_coldfront_project_not_found(self, mock_get_allocation_data):
        """What happens when an invoice project is not found in Coldfront."""
        mock_get_allocation_data.return_value = self._get_mock_allocation_data(
            ["P1", "P2"],
            ["PI1", "PI1"],
            ["IC1", "IC2"],
        )
        test_nonbillable_projects = ["P3"]
        test_invoice = self._get_test_invoice(["P1", "P2", "P3", "P4", "P5"])
        test_coldfront_fetch_proc = test_utils.new_coldfront_fetch_processor(
            data=test_invoice, nonbillable_projects=test_nonbillable_projects
        )
        test_coldfront_fetch_proc.process()
        output_invoice =  test_coldfront_fetch_proc.data
        answer_invoice = self._get_test_invoice(
            ["P1", "P2", "P3", "P4", "P5"],
            allocation_project_name=["P1-name", "P2-name", "P3", "P4", "P5"],
            pi=["PI1", "PI1", "", "", ""],
            institute_code=["IC1", "IC2", "", "", ""],
        )
        self.assertTrue(output_invoice.equals(answer_invoice))
