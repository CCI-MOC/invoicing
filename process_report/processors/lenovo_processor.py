from dataclasses import dataclass


from process_report.invoices import invoice
from process_report.processors import processor


@dataclass
class LenovoProcessor(processor.Processor):
    SU_CHARGE_MAP = {
        "OpenShift GPUA100SXM4": 1,
        "OpenStack GPUA100SXM4": 1,
        "OpenStack GPUH100": 2.74,
        "BM GPUH100": 2.74,
    }

    def _process(self):
        self.data[invoice.SU_CHARGE_FIELD] = self.data[invoice.SU_TYPE_FIELD].map(
            lambda x: self.SU_CHARGE_MAP.get(x, 0)
        )
        self.data[invoice.LENOVO_CHARGE_FIELD] = (
            self.data[invoice.SU_HOURS_FIELD] * self.data[invoice.SU_CHARGE_FIELD]
        )
