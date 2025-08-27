from dataclasses import dataclass

from process_report.invoices import invoice
from process_report.processors import processor


@dataclass
class ValidateClusterNameProcessor(processor.Processor):
    CLUSTER_NAME_MAP = {
        "NERC": "stack",
        "NERC-OCP": "ocp-prod",
        "NERC-OCP-EDU": "academic",
    }

    def _process(self):
        self.data[invoice.CLUSTER_NAME_FIELD] = self.data[
            invoice.CLUSTER_NAME_FIELD
        ].apply(lambda x: self.CLUSTER_NAME_MAP.get(x, x))
