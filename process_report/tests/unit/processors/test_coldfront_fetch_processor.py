from unittest import mock

from process_report.tests import base, util as test_utils


class TestColdfrontFetchProcessor(base.BaseTestCase):
    def _get_test_invoice(
        self,
        allocation_project_id,
        allocation_project_name="",
        pi="",
        institute_code="",
        cluster_name="",
    ):
        return self._create_test_invoice(
            {
                "Manager (PI)": pi,
                "Project - Allocation": allocation_project_name,
                "Project - Allocation ID": allocation_project_id,
                "Institution - Specific Code": institute_code,
                "Cluster Name": cluster_name,
            }
        )

    def _get_mock_allocation_data(self, project_id_list, pi_list, institute_code_list):
        mock_data = []
        for i, project in enumerate(project_id_list):
            mock_data.append(
                {
                    "project": {
                        "pi": pi_list[i],
                    },
                    "attributes": {
                        "Allocated Project ID": project,
                        "Allocated Project Name": f"{project}-name",
                        "Institution-Specific Code": institute_code_list[i],
                    },
                }
            )

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
            ["P1-name", "P1-name", "P2-name", "P3-name", "P4-name"],
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

        with self.assertRaises(ValueError) as cm:
            test_coldfront_fetch_proc.process()

        self.assertEqual(
            str(cm.exception),
            f"Projects {answer_project_set} not found in Coldfront and are billable! Please check the project names",
        )

    @mock.patch(
        "process_report.processors.coldfront_fetch_processor.ColdfrontFetchProcessor._fetch_coldfront_allocation_api",
    )
    def test_nonbillable_clusters(self, mock_get_allocation_data):
        """No errors are raised when an invoice project belonging
        to a non billable cluster (ocp-test) is not found in Coldfront"""
        mock_get_allocation_data.return_value = self._get_mock_allocation_data(
            ["P1", "P2"],
            ["PI1", "PI1"],
            ["IC1", "IC2"],
        )
        test_invoice = self._get_test_invoice(
            ["P1", "P2", "P3", "P4"],
            cluster_name=["ocp-prod", "stack", "ocp-test", "ocp-test"],
        )
        answer_invoice = self._get_test_invoice(
            ["P1", "P2", "P3", "P4"],
            ["P1-name", "P2-name", "", ""],
            ["PI1", "PI1", "", ""],
            ["IC1", "IC2", "", ""],
            ["ocp-prod", "stack", "ocp-test", "ocp-test"],
        )
        test_coldfront_fetch_proc = test_utils.new_coldfront_fetch_processor(
            data=test_invoice
        )
        test_coldfront_fetch_proc.process()
        output_invoice = test_coldfront_fetch_proc.data
        self.assertTrue(output_invoice.equals(answer_invoice))
