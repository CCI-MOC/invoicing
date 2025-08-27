import yaml
import pytest

from process_report.institute_list_validate import main
from process_report.tests.base import BaseTestCaseWithTempDir


class TestInstituteListValidate(BaseTestCaseWithTempDir):
    def test_valid_institute_list(self):
        test_institute_list = [
            {
                "display_name": "i1",
                "domains": ["i1.edu"],
                "mghpcc_partnership_start_date": "2022-01",
                "include_in_nerc_total_invoice": True,
            },
            {
                "display_name": "i2",
                "domains": ["i2.edu"],
            },
        ]

        test_file = self.tempdir / "institute_list.yaml"
        with open(test_file, "w") as f:
            yaml.dump(test_institute_list, f)
            f.flush()
            main(["--github", str(test_file)])

    def test_invalid_institute_list(self):
        test_institute_list = [
            {
                "display_name": "i1",
                "domains": ["i1.edu"],
                "mghpcc_partnership_start_date": "2022-01",
                "include_in_Nerc_total_invoice": True,  # Typo in key name
            }
        ]

        test_file = self.tempdir / "institute_list.yaml"
        with open(test_file, "w") as f:
            yaml.dump(test_institute_list, f)
            f.flush()
            with pytest.raises(SystemExit):
                main(["--github", str(test_file)])
