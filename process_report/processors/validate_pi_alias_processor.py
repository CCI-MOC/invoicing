from dataclasses import dataclass, field

from process_report import config
from process_report.invoices import invoice
from process_report.processors import processor


@dataclass
class ValidatePIAliasProcessor(processor.Processor):
    alias_map: dict = field(default_factory=config.get_alias_map)

    def _validate_pi_aliases(self):
        for pi, pi_aliases in self.alias_map.items():
            self.data.loc[
                self.data[invoice.PI_FIELD].isin(pi_aliases), invoice.PI_FIELD
            ] = pi

    def _process(self):
        self._validate_pi_aliases()
