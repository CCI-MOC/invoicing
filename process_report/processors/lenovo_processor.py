from dataclasses import dataclass


from process_report.invoices import invoice
from process_report.processors import processor


@dataclass
class LenovoProcessor(processor.Processor):
    SU_CHARGE_MULTIPLIER = 1

    def _process(self):
        self._create_column(
            invoice.SU_CHARGE_FIELD,
            int,
            self.SU_CHARGE_MULTIPLIER,
        )
        self._create_column(
            invoice.LENOVO_CHARGE_FIELD,
            invoice.BALANCE_FIELD_TYPE,
            self.data[invoice.SU_HOURS_FIELD] * self.data[invoice.SU_CHARGE_FIELD],
        )
