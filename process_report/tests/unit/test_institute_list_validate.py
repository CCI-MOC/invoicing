import yaml
import pytest

from process_report.institute_list_validate import main
from process_report.institute_list_models import InstituteList
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

    def test_get_pi_institution(self):
        domain_map = {
            "harvard.edu": "Harvard University",
            "bu.edu": "Boston University",
            "bentley.edu": "Bentley",
            "mclean.harvard.edu": "McLean Hospital",
            "northeastern.edu": "Northeastern University",
            "childrens.harvard.edu": "Boston Children's Hospital",
            "meei.harvard.edu": "Massachusetts Eye & Ear",
            "dfci.harvard.edu": "Dana-Farber Cancer Institute",
            "bwh.harvard.edu": "Brigham and Women's Hospital",
            "bidmc.harvard.edu": "Beth Israel Deaconess Medical Center",
        }
        test_institute_list = InstituteList([])
        test_institute_list.domain_institute_mapping = domain_map

        answers = {
            "q@bu.edu": "Boston University",
            "c@mclean.harvard.edu": "McLean Hospital",
            "b@harvard.edu": "Harvard University",
            "e@edu": "",
            "pi@northeastern.edu": "Northeastern University",
            "h@a.b.c.harvard.edu": "Harvard University",
            "c@a.childrens.harvard.edu": "Boston Children's Hospital",
            "d@a-b.meei.harvard.edu": "Massachusetts Eye & Ear",
            "e@dfci.harvard": "",
            "f@bwh.harvard.edu": "Brigham and Women's Hospital",
            "g@bidmc.harvard.edu": "Beth Israel Deaconess Medical Center",
        }

        for pi_email, answer in answers.items():
            assert test_institute_list.get_institution_from_pi(pi_email) == answer
