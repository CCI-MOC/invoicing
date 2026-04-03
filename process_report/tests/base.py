import tempfile
import shutil
from pathlib import Path
from unittest import TestCase

import pandas


INVOICE_FIELD_LIST = [
    "Invoice Month",
    "Project - Allocation",
    "Project - Allocation ID",
    "Manager (PI)",
    "Invoice Email",
    "Invoice Address",
    "Institution",
    "Institution - Specific Code",
    "Prepaid Group Name",
    "Prepaid Group Institution",
    "Prepaid Group Balance",
    "Prepaid Group Used",
    "SU Hours (GBhr or SUhr)",
    "SU Type",
    "SU Charge",
    "Charge",
    "Rate",
    "Cost",
    "Credit",
    "Credit Code",
    "Subsidy",
    "Balance",
    # Internally used fields
    "Is Billable",
    "Missing PI",
    "PI Balance",
    "Project",
    "MGHPCC Managed",
    "Cluster Name",
    "Is Course",
]


class BaseTesCase(TestCase):
    def create_test_invoice(self, data_dict: dict):
        return pandas.DataFrame(data_dict, columns=INVOICE_FIELD_LIST)


class BaseTestCaseWithTempDir(BaseTesCase):
    def setUp(self):
        self.tempdir = Path(tempfile.TemporaryDirectory(delete=False).name)

    def tearDown(self):
        shutil.rmtree(self.tempdir)
