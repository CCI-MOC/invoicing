from unittest import TestCase
import pandas

from process_report.tests import util as test_utils


class TestLenovoProcessor(TestCase):
    def test_process_lenovo(self):
        test_su_charge_info = {"GPUA100SXM4": 1, "GPUH100": 2.5}
        test_invoice = pandas.DataFrame(
            {
                "SU Type": ["OpenStack GPUA100SXM4"] * 2
                + ["OpenShift GPUA100SXM4"] * 2
                + ["Not Lenovo SU", "Random GPUH100"],
                "SU Hours (GBhr or SUhr)": [1, 10, 100, 4, 432, 10],
            }
        )
        answer_invoice = test_invoice.copy()
        answer_invoice["SU Charge"] = [1, 1, 1, 1, 0, 2.5]
        answer_invoice["Charge"] = (
            answer_invoice["SU Hours (GBhr or SUhr)"] * answer_invoice["SU Charge"]
        )

        lenovo_proc = test_utils.new_lenovo_processor(
            data=test_invoice, su_charge_info=test_su_charge_info
        )
        lenovo_proc.process()
        assert lenovo_proc.data.equals(answer_invoice)
