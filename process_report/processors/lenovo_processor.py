from dataclasses import dataclass


from process_report.invoices import invoice
from process_report.processors import processor


@dataclass
class LenovoProcessor(processor.Processor):
    su_charge_info: dict

    def _apply_su_charge(self, data):
        for su_name, su_charge in self.su_charge_info.items():
            if su_name in data:
                return su_charge
        return 0

    def _process(self):
        self.data[invoice.SU_CHARGE_FIELD] = self.data[invoice.SU_TYPE_FIELD].apply(
            self._apply_su_charge
        )
        self.data[invoice.LENOVO_CHARGE_FIELD] = (
            self.data[invoice.SU_HOURS_FIELD] * self.data[invoice.SU_CHARGE_FIELD]
        )
