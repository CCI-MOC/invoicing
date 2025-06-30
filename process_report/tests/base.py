import tempfile
import shutil
from pathlib import Path
from unittest import TestCase

import pandas
import pyarrow


### Invoice column types
BOOL_FIELD_TYPE = pandas.BooleanDtype()
STRING_FIELD_TYPE = pandas.StringDtype()
BALANCE_FIELD_TYPE = pandas.ArrowDtype(pyarrow.decimal128(21, 2))
###


class BaseTestCase(TestCase):
    field_type_mapping = {
        "Is Billable": BOOL_FIELD_TYPE,
        "Missing PI": BOOL_FIELD_TYPE,
        "Rate": STRING_FIELD_TYPE,
        "Cost": BALANCE_FIELD_TYPE,
        "Credit": BALANCE_FIELD_TYPE,
        "Subsidy": BALANCE_FIELD_TYPE,
        "Balance": BALANCE_FIELD_TYPE,
        "Prepaid Group Managed": BOOL_FIELD_TYPE,
        "Prepaid Group Balance": BALANCE_FIELD_TYPE,
        "Prepaid Group Used": BALANCE_FIELD_TYPE,
    }

    def _create_test_invoice(self, data: dict) -> pandas.DataFrame:
        """
        Given a dictionary of data, create a DataFrame that represents an invoice.
        Also standardizes the type for some columns mostly those that expect monetary
        values, like BALANCE_FIELD, COST_FIELD, etc. These columns are usually
        converted to pyarrow.decimal128(21, 2)
        """
        standard_dtypes = {}
        for column_name in data.keys():
            if column_name in self.field_type_mapping:
                standard_dtypes[column_name] = self.field_type_mapping[column_name]

        test_invoice = pandas.DataFrame(data)
        return test_invoice.astype(standard_dtypes)


class BaseTestCaseWithTempDir(BaseTestCase):
    def setUp(self):
        self.tempdir = Path(tempfile.TemporaryDirectory(delete=False).name)

    def tearDown(self):
        shutil.rmtree(self.tempdir)
