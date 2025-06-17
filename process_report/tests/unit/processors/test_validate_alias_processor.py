from process_report.tests import base, util as test_utils


class TestValidateAliasProcessor(base.BaseTestCase):
    def test_validate_alias(self):
        alias_map = {"PI1": ["PI1_1", "PI1_2"], "PI2": ["PI2_1"]}
        test_data = self._create_test_invoice(
            {
                "Manager (PI)": ["PI1", "PI1_1", "PI1_2", "PI2_1", "PI2_1"],
            }
        )
        answer_data = self._create_test_invoice(
            {
                "Manager (PI)": ["PI1", "PI1", "PI1", "PI2", "PI2"],
            }
        )

        validate_pi_alias_proc = test_utils.new_validate_pi_alias_processor(
            data=test_data, alias_map=alias_map
        )
        validate_pi_alias_proc.process()
        self.assertTrue(answer_data.equals(validate_pi_alias_proc.data))
