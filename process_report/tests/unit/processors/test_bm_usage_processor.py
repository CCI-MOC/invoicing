from unittest import TestCase

import pandas

from process_report.tests import util as test_utils


class TestBUSubsidyProcessor(TestCase):
    def test_process_bm_usage(self):
        test_invoice = pandas.DataFrame(
            {
                "Project - Allocation": ["test", "test bm-bm", "not-bm"],
                "Project - Allocation ID": [None] * 3,
                "Invoice Email": [None] * 3,
                "Cluster Name": ["bm", "bm", "ocp"],
            }
        )

        answer_invoice = pandas.DataFrame(
            {
                "Project - Allocation": [
                    "test BM Usage",
                    "test bm-bm BM Usage",
                    "not-bm",
                ],
                "Project - Allocation ID": ["ESI Bare Metal"] * 2 + [None],
                "Invoice Email": ["nclinton@bu.edu"] * 2 + [None],
                "Cluster Name": ["bm", "bm", "ocp"],
            }
        )

        bm_usage_proc = test_utils.new_bm_usage_processor(data=test_invoice)
        bm_usage_proc.process()
        self.assertTrue(bm_usage_proc.data.equals(answer_invoice))
