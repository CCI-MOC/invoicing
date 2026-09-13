import csv
import pandas

QUOTECHAR = "|"
QUOTING = csv.QUOTE_MINIMAL


def _invoice_dtypes():
    from process_report.invoices import invoice

    return {
        col.name: col.dtype
        for col in vars(invoice).values()
        if isinstance(col, invoice.InvoiceColumn)
    }


def write_invoice_csv(df, filepath):
    df.to_csv(filepath, index=False, quotechar=QUOTECHAR, quoting=QUOTING)


def read_invoice_csv(filepath):
    return pandas.read_csv(
        filepath, quotechar=QUOTECHAR, dtype=_invoice_dtypes(), engine="pyarrow"
    )
