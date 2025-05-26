from process_report.tests import base, util as test_utils


class TestLenovoProcessor(base.BaseTestCase):
    def test_process_lenovo(self):
        test_invoice = self._create_test_invoice(
            {
                "SU Hours (GBhr or SUhr)": [1, 10, 100, 4, 432, 10],
            }
        )
        answer_invoice = test_invoice.copy()
        answer_invoice["SU Charge"] = 1
        answer_invoice["Charge"] = (
            answer_invoice["SU Hours (GBhr or SUhr)"] * answer_invoice["SU Charge"]
        )

        lenovo_proc = test_utils.new_lenovo_processor(data=test_invoice)
        lenovo_proc.process()
        output_invoice = lenovo_proc.data

        answer_invoice = answer_invoice.astype(output_invoice.dtypes)
        self.assertTrue(output_invoice.equals(answer_invoice))
