from unittest import TestCase
import yaml
import tempfile

from process_report.institute_list_validate import main


class TestInstituteListValidate(TestCase):
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

        with tempfile.NamedTemporaryFile(mode="w") as f:
            yaml.dump(test_institute_list, f)
            f.flush()
            main(["--github", f.name])

    def test_invalid_institute_list(self):
        test_institute_list = [
            {
                "display_name": "i1",
                "domains": ["i1.edu"],
                "mghpcc_partnership_start_date": "2022-01",
                "include_in_Nerc_total_invoice": True,  # Typo in key name
            }
        ]

        with tempfile.NamedTemporaryFile(mode="w") as f:
            yaml.dump(test_institute_list, f)
            f.flush()
            with self.assertRaises(SystemExit):
                main(["--github", f.name])
