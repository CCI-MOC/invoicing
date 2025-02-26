from dataclasses import dataclass

import pandas

from process_report.invoices import invoice
from process_report.processors import processor


@dataclass
class BMUsageProcessor(processor.Processor):
    def _get_bm_project_mask(self):
        return pandas.Series(True, index=self.data.index)  # TODO: Remove dummy mask

    def _process(self):
        bm_projects_mask = self._get_bm_project_mask()
        self.data.loc[bm_projects_mask, invoice.PROJECT_FIELD] = self.data.loc[
            bm_projects_mask, invoice.PROJECT_FIELD
        ].apply(lambda v: v + " BM Usage")
        self.data.loc[bm_projects_mask, invoice.PROJECT_ID_FIELD] = "ESI Bare Metal"
        self.data.loc[bm_projects_mask, invoice.INVOICE_EMAIL_FIELD] = "nclinton@bu.edu"
