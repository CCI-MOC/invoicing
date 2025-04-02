from unittest import TestCase, mock
import pandas

from process_report.tests import util as test_utils


class TestColdfrontFetchProcessor(TestCase):
    def _get_test_invoice(
        self,
        project,
        pi=None,
        institute_code=None,
    ):
        if not pi:
            pi = [""] * len(project)

        if not institute_code:
            institute_code = [""] * len(project)

        return pandas.DataFrame(
            {
                "Manager (PI)": pi,
                "Project - Allocation": project,
                "Institution - Specific Code": institute_code,
            }
        )

    def _get_mock_allocation_data(self, project_list, pi_list, institute_code_list):
        mock_data = {}
        for i, project in enumerate(project_list):
            mock_data[project] = {}
            mock_data[project]["project"] = {"pi": pi_list[i]}
            mock_data[project]["attributes"] = {
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
            ["PI1", "PI1", "PI1", "", "PI12"],
            ["IC1", "IC1", "", "", "IC2"],
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
        answer_project_set = ["P4", "P5"]
        test_coldfront_fetch_proc = test_utils.new_coldfront_fetch_processor(
            data=test_invoice, nonbillable_projects=test_nonbillable_projects
        )

        with self.assertLogs() as log:
            test_coldfront_fetch_proc.process()
            self.assertIn(
                f"Projects {answer_project_set} not found in Coldfront and are billable! Please check the project names",
                log.output[0],
            )

    @mock.patch("requests.sessions.Session.get")
    def test_query_url(self, mock_request_get):
        test_coldfront_fetch_proc = test_utils.new_coldfront_fetch_processor()
        test_coldfront_fetch_proc.coldfront_client = mock.MagicMock()
        test_coldfront_fetch_proc._fetch_coldfront_allocation_api(["P1", "P2"])
        test_coldfront_fetch_proc.coldfront_client.get.assert_called_with(
            "https://coldfront.mss.mghpcc.org/api/allocations?attr_Allocated Project Name=P1&attr_Allocated Project Name=P2"
        )
