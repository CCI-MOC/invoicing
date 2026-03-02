from dataclasses import dataclass
import logging

from process_report.invoices import invoice
from process_report.processors import processor


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass
class SpecialBillingRulesProcessor(processor.Processor):
    _EMRE_EMAIL = "emre_keskin@harvard.edu"
    _OPENSTACK_STORAGE_SU_TYPE = "Openstack Storage"
    _EMRE_STORAGE_CREDIT_CODE = "0005"
    _GRIOT_GRITS_PROJECT = "griot-grits-aa488b"

    def _process(self):
        self._apply_emre_openstack_storage_credit()
        self._apply_griot_grits_billable()

    def _apply_emre_openstack_storage_credit(self):
        email = self.data[invoice.INVOICE_EMAIL_FIELD].fillna("").str.strip()
        su_type = self.data[invoice.SU_TYPE_FIELD].fillna("").str.strip()

        rule_mask = (email == self._EMRE_EMAIL) & (
            su_type == self._OPENSTACK_STORAGE_SU_TYPE
        )

        self.data.loc[rule_mask, invoice.CREDIT_CODE_FIELD] = (
            self._EMRE_STORAGE_CREDIT_CODE
        )
        self.data.loc[rule_mask, invoice.CREDIT_FIELD] = self.data.loc[
            rule_mask, invoice.COST_FIELD
        ]
        self.data.loc[rule_mask, invoice.PI_BALANCE_FIELD] = 0
        self.data.loc[rule_mask, invoice.BALANCE_FIELD] = 0

    def _apply_griot_grits_billable(self):
        project = self.data[invoice.PROJECT_FIELD].fillna("").str.strip()
        rule_mask = project == self._GRIOT_GRITS_PROJECT
        self.data.loc[rule_mask, invoice.IS_BILLABLE_FIELD] = True
