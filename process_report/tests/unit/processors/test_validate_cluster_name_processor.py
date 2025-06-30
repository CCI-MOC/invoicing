from unittest import TestCase
import pandas

from process_report.tests import util as test_utils


class TestValidateClusterNameProcessor(TestCase):
    def test_validate_cluster_name(self):
        test_invoice = pandas.DataFrame(
            {"Cluster Name": ["NERC", "NERC-OCP", "bm", "random"]}
        )

        answer_invoice = test_invoice.copy()
        answer_invoice["Cluster Name"] = ["stack", "ocp-prod", "bm", "random"]

        validate_proc = test_utils.new_validate_cluster_name_processor(
            data=test_invoice
        )
        validate_proc.process()

        self.assertTrue(validate_proc.data.equals(answer_invoice))
