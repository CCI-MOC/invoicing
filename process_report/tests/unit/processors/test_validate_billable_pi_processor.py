from unittest import TestCase
import pandas
import uuid
import math

from process_report.tests import util as test_utils


class TestValidateBillablePIProcessor(TestCase):
    def test_remove_nonbillables(self):
        pis = [uuid.uuid4().hex for _ in range(10)]
        projects = [uuid.uuid4().hex for _ in range(10)]
        cluster_names = [uuid.uuid4().hex for _ in range(10)]
        cluster_names[6:8] = ["ocp-test"] * 2  # Test that ocp-test is not billable
        institutions = ["Test University"] * len(pis)
        is_course = [False] * len(pis)
        nonbillable_pis = pis[:3]
        nonbillable_projects = [
            project.upper() for project in projects[7:]
        ]  # Test for case-insentivity
        billable_pis = pis[3:6]

        data = pandas.DataFrame(
            {
                "Manager (PI)": pis,
                "Project - Allocation": projects,
                "Cluster Name": cluster_names,
                "Is Course": is_course,
                "Institution": institutions,
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
        assert data[data["Manager (PI)"].isin(nonbillable_pis)].empty
        assert data[data["Project - Allocation"].isin(nonbillable_projects)].empty
        assert data[data["Cluster Name"] == "ocp-test"].empty
        assert data["Manager (PI)"].tolist() == billable_pis

    def test_empty_pi_name(self):
        test_data = pandas.DataFrame(
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
                "Institution": ["Test University"] * 5,
                "Is Course": [False] * 5,
            }
        )
        assert len(test_data[pandas.isna(test_data["Manager (PI)"])]) == 1
        validate_billable_pi_proc = test_utils.new_validate_billable_pi_processor(
            data=test_data
        )
        validate_billable_pi_proc.process()
        output_data = validate_billable_pi_proc.data
        output_data = output_data[~output_data["Missing PI"]]
        assert len(output_data[pandas.isna(output_data["Manager (PI)"])]) == 0

    def test_is_course_marks_nonbillable(self):
        """Rows with Is Course == True and is BU should be marked nonbillable; False should be billable."""
        pis = [uuid.uuid4().hex for _ in range(4)]
        projects = [uuid.uuid4().hex for _ in range(4)]
        cluster_names = ["test-cluster"] * 4
        # Only first project should be nonbillable
        is_course = [True, False, True, False]
        institutions = ["Boston University", "Boston University", "test", "test"]

        test_data = pandas.DataFrame(
            {
                "Manager (PI)": pis,
                "Project - Allocation": projects,
                "Cluster Name": cluster_names,
                "Is Course": is_course,
                "Institution": institutions,
            }
        )

        validate_proc = test_utils.new_validate_billable_pi_processor(
            data=test_data, nonbillable_pis=[], nonbillable_projects=[]
        )
        validate_proc.process()
        output = validate_proc.data

        expected_billable = [False, True, True, True]
        actual_billable = output["Is Billable"].tolist()
        assert actual_billable == expected_billable
