from unittest import TestCase

import pandas

from process_report.tests import util as test_utils


class TestBUSubsidyProcessor(TestCase):
    def test_get_bm_project_mask(self):
        test_invoice = pandas.DataFrame({})

        answer_invoice = test_invoice.iloc[[0, 2]]

        bm_usage_proc = test_utils.new_bm_usage_processor(data=test_invoice)
        bm_project_mask = bm_usage_proc._get_bm_project_mask()
        self.assertTrue(test_invoice[bm_project_mask].equals(answer_invoice))

    def test_process_bm_usage(self):
        test_invoice = pandas.DataFrame(
            {
                "Project - Allocation": ["test", "test bm-bm"],
                "Project - Allocation ID": [None] * 2,
                "Invoice Email": [None] * 2,
            }
        )

        answer_invoice = pandas.DataFrame(
            {
                "Project - Allocation": ["test BM Usage", "test bm-bm BM Usage"],
                "Project - Allocation ID": ["ESI Bare Metal"] * 2,
                "Invoice Email": ["nclinton@bu.edu"] * 2,
            }
        )

        bm_usage_proc = test_utils.new_bm_usage_processor(data=test_invoice)
        bm_usage_proc.process()
        self.assertTrue(bm_usage_proc.data.equals(answer_invoice))
