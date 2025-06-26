import pandas
import uuid
import math

from process_report.tests import base, util as test_utils


class TestValidateBillablePIProcessor(base.BaseTestCase):
    def test_remove_nonbillables(self):
        pis = [uuid.uuid4().hex for x in range(10)]
        projects = [uuid.uuid4().hex for x in range(10)]
        cluster_names = [uuid.uuid4().hex for x in range(10)]
        cluster_names[6:8] = ["ocp-test"] * 2  # Test that ocp-test is not billable
        nonbillable_pis = pis[:3]
        nonbillable_projects = [
            project.upper() for project in projects[7:]
        ]  # Test for case-insentivity
        billable_pis = pis[3:6]

        data = self._create_test_invoice(
            {
                "Manager (PI)": pis,
                "Project - Allocation": projects,
                "Cluster Name": cluster_names,
            }
        )

        validate_billable_pi_proc = test_utils.new_validate_billable_pi_processor(
            data=data,
            nonbillable_pis=nonbillable_pis,
            nonbillable_projects=nonbillable_projects,
        )
        validate_billable_pi_proc.process()
        data = validate_billable_pi_proc.data
        data = data[data["Is Billable"]]
        self.assertTrue(data[data["Manager (PI)"].isin(nonbillable_pis)].empty)
        self.assertTrue(
            data[data["Project - Allocation"].isin(nonbillable_projects)].empty
        )
        self.assertTrue(data[data["Cluster Name"] == "ocp-test"].empty)
        self.assertTrue(data["Manager (PI)"].tolist() == billable_pis)

    def test_empty_pi_name(self):
        test_data = self._create_test_invoice(
            {
                "Manager (PI)": ["PI1", math.nan, "PI1", "PI2", "PI2"],
                "Project - Allocation": [
                    "ProjectA",
                    "ProjectB",
                    "ProjectC",
                    "ProjectD",
                    "ProjectE",
                ],
                "Cluster Name": ["test-cluster"] * 5,
            }
        )
        self.assertEqual(1, len(test_data[pandas.isna(test_data["Manager (PI)"])]))
        validate_billable_pi_proc = test_utils.new_validate_billable_pi_processor(
            data=test_data
        )
        validate_billable_pi_proc.process()
        output_data = validate_billable_pi_proc.data
        output_data = output_data[~output_data["Missing PI"]]
        self.assertEqual(0, len(output_data[pandas.isna(output_data["Manager (PI)"])]))
