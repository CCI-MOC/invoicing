# NERC Invoicing

Each month, a set of invoices is generated for usage on the NERC Openshift and Openstack cluster. Due to various administrative needs, these invoices need further processing. As of time of writing, below is an exhaustive ordered list of processing steps performed by scripts in this repo:
1. Combine all provided invoices into one `Pandas` dataframe
2. Check for PI aliases, using the alias file stored in S3
3. Add each PI's institution name, using [institute.yaml](process_report/institute_list.yaml)
4. Add info for the Lenovo invoice
5. Mark projects that have no PI name, or is not billable, using files stored in a private MOCA repo
6. Apply the New-PI credit, using the old-PI file in S3
7. Apply the BU internal subsidy, based on a [subsidy value](https://github.com/CCI-MOC/nerc-rates/blob/5b24d3a28b0f3ee0516a329ad8ccb555cb689091/rates.yaml#L249) set in `nerc_rates`
8. Apply prepayments, also with files stored in a pricate MOC repo

Each processing step is implemented in a [`Processor`](./process_report/processors/processor.py) subclass. For example, applying the New-PI credit is done by the [`NewPICreditProcessor`](./process_report/processors/new_pi_credit_processor.py). More details on each processing step can be found below or in their respective `Processor` subclass.

After processing is done, the processed dataframe is filtered and exported into various invoices. These can be exported in any order:
1. Lenovo CSV invoice
2. nonbillable and billable CSV invoices
3. NERC Total CSV invoice
4. MOCA prepaid CSV invoice
5. PI-specific PDF invoices
6. BU-Internal CSV invoice

The filtering and exporting of invoices are done in [`Invoice`](./process_report/invoices/invoice.py) subclasses, such as the [`LenovoInvoice`](./process_report/invoices/lenovo_invoice.py)

## Try it yourself
The entrypoint of the invoicing system is [`process_report.py`](./process_report/process_report.py). To test the entire invoicing pipeline end-to-end:

```
python -m process_report.process_report
usage: process_report.py [-h] [--fetch-from-s3] [--upload-to-s3] --invoice-month INVOICE_MONTH --pi-file PI_FILE --projects-file PROJECTS_FILE --timed-projects-file TIMED_PROJECTS_FILE [--prepay-credits PREPAY_CREDITS]
                         [--prepay-projects PREPAY_PROJECTS] [--prepay-contacts PREPAY_CONTACTS] [--nonbillable-file NONBILLABLE_FILE] [--output-file OUTPUT_FILE] [--output-folder OUTPUT_FOLDER] [--BU-invoice-file BU_INVOICE_FILE]
                         [--NERC-total-invoice-file NERC_TOTAL_INVOICE_FILE] [--Lenovo-file LENOVO_FILE] [--old-pi-file OLD_PI_FILE] [--alias-file ALIAS_FILE] [--prepay-debits PREPAY_DEBITS]
                         [--new-pi-credit-amount NEW_PI_CREDIT_AMOUNT] [--BU-subsidy-amount BU_SUBSIDY_AMOUNT]
                         [csv_files ...]
process_report.py: error: the following arguments are required: --invoice-month, --pi-file, --projects-file, --timed-projects-file

E.g. python -m process_report.process_report test1.csv test2.csv --invoice-month 2024-02 --pi-file pi.txt --projects-file projects.txt --timed-projects-file timed_projects.txt
```

The input CSV invoices must at least contain the following headers:
- Invoice Month
- Project - Allocation
- Manager (PI)
- Institution
- SU Hours (GBhr or SUhr)
- SU Type
- Cost

## Processing steps
Below are brief explanations of each processing step. These do not cover all implementation details or edge cases, especially for more complex processing steps like the credits and prepayments. Further explanation can be found in the various test cases or by reading the commit messages that introduced each processors.

### Combine CSVs
This script combines 3 separate Invoice data CSVs into 1 main dataframe, from which all other processing steps are applied. Currently, it combines
OpenShift SU, OpenStack SU, and Storage SU usage data.

### Checking PI aliases
In the past, it was found that a PI may have more than one username on the NERC. Therefore, checking for PI aliases are needed, and alias info is stored in an alias file in S3 storage.
A local file can also be provided to the CLI option `--alias-file`. An example alias file is:
```
pi1@bu.edu,pi1,PII1
pi2@bu.edu,pi2,pi_2
```
Where the first column is the PI name you'd want to appear on invoices, or the canonical PI name. All aliases in the invoices will be changed to the canonical name.

### Adding institution names
Each PI is affiliated with an institution. This affiliation is determined using the PI's username, which is generally an email address belonging to their institution.
I.e p1@bu.edu would be the username of a PI affiliated with Boston University.

This processor determines the institution of each PI using an email-to-institution mapping list [institute.yaml](process_report/institute_list.yaml)

### Adding Lenovo charge information
Certain [NERC service units](https://nerc-project.github.io/nerc-docs/get-started/cost-billing/how-pricing-works/#service-units-sus) (SUs) uses hardware loaned from Lenovo. As such, this processing step adds info needed to generate the invoice we will send to Lenovo

### Marking projects as nonbillable, or missing a PI name
Certain projects on NERC are nonbillable. Some are always nonbillable, or only nonbillable for a certain period of time. Some PIs are also exempt from billing.
All of this information is stored in a private MOCA repo. For testing with your own local files, making sure to provide the following:

A file containing list of nonbillable PIs for `--pi-file`:
```
alice@example.com
bob@example.com
foo
bar
```

A file containing list of nonbillable projects for `--projects-file`:

projects.txt
```
foo
bar
blah blah
```

A file containing list of timed projects for `--timed-projects-file`:
```
PI,Project,Start Date,End Date,Reason
alice@example.com,project foo,2023-09,2024-08,Internal
bo@example.com,project bar,2023-09,2024-08,Internal
```

With this set of example files, `project foo` will not be billed for September 2023 and August 2024 and all the months in between for total of 1 year.

### New PI Credit
A flat credit applied for new PIs on the NERC. An file containing a list of known PIs and their date of first appearace must be provided to the CLI option `--old-pi-file`.

The file of old pis may look like this:
```
PI,First Invoice Month,Initial Credits,1st Month Used,2nd Month Used
bob@bu.edu,2025-02,1000.00,0.01,0.00
alice@bu.edu,2025-01,1000.00,292.45,200.32
```

PIs only have 2 months to use their New-PI credit.

### BU-Internal subsidy
Every month, BU PIs are provided a flat subsidy. This subsidy amount can be provided through the CLI arguement `--BU-subsidy-amount`

### Prepayments
The invoincing system supports prepayments, where PIs or institutions can choose to prepay for the usage of a group of projects, and have this prepayment used up over time.
For prepayments to be properly processed, 4 files must be provided:
`--prepay-credits` - Containing the list of prepayments or "credits" each prepay group has
```
Month,Group Name,Credit
2024-01,G1,2000
2024-02,G2,5000
2024-05,G1,10000
```

`--prepay-debits` - Containing the list of spending or "debits" each prepay group has
```
Month,Group Name,Debit
2024-01,G1,1000
```

`--prepay-contacts` - A list providing the contact email for each group, and whether they're managed by the MGHPCC
```
Group Name,Group Contact Email,MGHPCC Managed
G1,g1@bu.edu,No
G2,g2@bu.edu,Yes
```

`--prepay-projects` - Listing the names and membership period of each prepay group's projects
```
Group Name,Project,Start Date,End Date
G1,P1,2024-01, 2024-06
G1,P2,2024-01, 2024-12
G2,P1,2024-01, 2024-06
```
