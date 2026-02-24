from unittest import TestCase
import pandas

from process_report.invoices import invoice
from process_report.processors import special_billing_rules_processor


class TestSpecialBillingRulesProcessor(TestCase):
    def test_applies_emre_openstack_storage_credit_overwrite(self):
        test_invoice = pandas.DataFrame(
            {
                # case 1: emre_keskin@harvard.edu, Openstack Storage
                # case 2: other@harvard.edu, Openstack Storage
                # case 3: emre_keskin@harvard.edu, Other SU Type
                invoice.INVOICE_EMAIL_FIELD: [
                    "emre_keskin@harvard.edu",
                    "other@harvard.edu",
                    "emre_keskin@harvard.edu",
                ],
                invoice.SU_TYPE_FIELD: [
                    "Openstack Storage",
                    "Openstack Storage",
                    "Other SU Type",
                ],
                invoice.COST_FIELD: [100.0, 75.0, 25.0],
                invoice.CREDIT_CODE_FIELD: ["0001", "0003", "0004"],
                invoice.CREDIT_FIELD: [10.0, 30.0, 40.0],
                invoice.PI_BALANCE_FIELD: [90.0, 45.0, -15.0],
                invoice.BALANCE_FIELD: [80.0, 50.0, -10.0],
            }
        )

        proc = special_billing_rules_processor.SpecialBillingRulesProcessor(
            "0000-00", test_invoice, ""
        )
        proc.process()

        self.assertEqual(
            proc.data.loc[0, invoice.CREDIT_CODE_FIELD],
            special_billing_rules_processor.SpecialBillingRulesProcessor._EMRE_STORAGE_CREDIT_CODE,
        )
        self.assertEqual(proc.data.loc[0, invoice.CREDIT_FIELD], 100.0)
        self.assertEqual(proc.data.loc[0, invoice.PI_BALANCE_FIELD], 0)
        self.assertEqual(proc.data.loc[0, invoice.BALANCE_FIELD], 0)

        self.assertEqual(proc.data.loc[1, invoice.CREDIT_CODE_FIELD], "0003")
        self.assertEqual(proc.data.loc[1, invoice.CREDIT_FIELD], 30.0)
        self.assertEqual(proc.data.loc[1, invoice.PI_BALANCE_FIELD], 45.0)
        self.assertEqual(proc.data.loc[1, invoice.BALANCE_FIELD], 50.0)

        self.assertEqual(proc.data.loc[2, invoice.CREDIT_CODE_FIELD], "0004")
        self.assertEqual(proc.data.loc[2, invoice.CREDIT_FIELD], 40.0)
        self.assertEqual(proc.data.loc[2, invoice.PI_BALANCE_FIELD], -15.0)
        self.assertEqual(proc.data.loc[2, invoice.BALANCE_FIELD], -10.0)
