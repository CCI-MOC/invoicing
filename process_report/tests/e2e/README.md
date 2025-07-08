# End-to-End (E2E) Tests

This directory contains end-to-end tests for the NERC invoicing pipeline. These tests are designed to validate that the entire pipeline runs successfully from start to finish with realistic data.

## Purpose

The E2E tests are **not** intended to cover all edge cases or detailed validation - that's what unit tests are for. Instead, they:

1. **Validate Pipeline Execution**: Ensure the full pipeline runs without errors
2. **Check Basic Outputs**: Verify that all expected output files are generated
3. **Structural Validation**: Perform basic sanity checks on output file structures
4. **Integration Testing**: Test that all pipeline components work together correctly

## What the E2E Test Covers

The main E2E test (`test_e2e_pipeline.py`) tests the following pipeline stages:

### Input Processing
- Combines multiple CSV invoice files (OpenShift, OpenStack, Storage)
- Processes PI aliases and institution mappings
- Handles non-billable PIs and projects
- Processes timed projects and prepayment data

### Core Processing
- Fetches data from Coldfront (mocked in tests)
- Applies credits and subsidies
- Processes prepayments and group billing
- Generates all invoice types

### Output Generation
- Billable invoice (`NERC 2024-01.csv`)
- Non-billable invoice (`NERC (Nonbillable) 2024-01.csv`)
- NERC total invoice (`NERC-2024-01-Total-Invoice.csv`)
- BU internal invoice (`NERC BU 2024-01.csv`)
- Lenovo invoice (`Lenovo 2024-01.csv`)
- MOCA prepaid invoice (`MOCA-A_Prepaid_Groups-2024-01-Invoice.csv`)
- Prepay credits snapshot (`NERC_Prepaid_Group-Credits-2024-01.csv`)
- PI-specific PDF invoices (generation is mocked)

## Test Data

The `test_data/` directory contains realistic test data that mimics production inputs:

- **CSV Invoice Files**: `test_invoice_openshift.csv`, `test_invoice_openstack.csv`, `test_invoice_storage.csv`
- **Configuration Files**: Non-billable PIs/projects, timed projects, prepay configurations
- **Historical Data**: Old PI file, alias mappings, prepay debits

## Running the Tests

### Prerequisites

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Install Chromium (for PDF generation, though it's mocked in tests):
   ```bash
   # Ubuntu/Debian
   sudo apt-get install chromium-browser

   # macOS
   brew install chromium
   ```

### Local Execution

Run the E2E tests locally:

```bash
# From the project root directory
python -m unittest discover -s process_report/tests/e2e

# Or run the specific test file
python -m unittest process_report.tests.e2e.test_e2e_pipeline

# Or run directly
python process_report/tests/e2e/test_e2e_pipeline.py
```

### Environment Variables

The test sets these automatically, but you can override if needed:

```bash
export KEYCLOAK_CLIENT_ID=test
export KEYCLOAK_CLIENT_SECRET=test
export CHROME_BIN_PATH=/usr/bin/chromium-browser
```

## CI/CD Integration

The E2E tests run automatically in GitHub Actions via `.github/workflows/e2e-tests.yaml`:

- **Triggers**: On push to main, pull requests, and manual dispatch
- **Environment**: Ubuntu with Python 3.12 and Chromium
- **Artifacts**: Test outputs are uploaded on failure for debugging

## Test Output and Cleanup

The test:
1. Creates all output files in the project root (mimicking production)
2. Validates file existence and basic structure
3. Automatically cleans up generated files in `tearDown()`

If the test fails, check:
- The error message and stack trace
- Generated output files (uploaded as artifacts in CI)
- Pipeline logs for specific processing errors

## Extending the Tests

To add new validations:

1. **Output Files**: Add to `expected_outputs` list in `test_e2e_pipeline_execution()`
2. **Structural Checks**: Add to `_validate_output_file_structures()`
3. **Test Data**: Add new files to `test_data/` directory
4. **Pipeline Arguments**: Modify `_run_pipeline()` method

## Limitations

- **No S3 Integration**: S3 operations are mocked to avoid external dependencies
- **PDF Generation**: Chromium calls are mocked but directory structure is validated
- **External APIs**: Coldfront and other API calls are mocked
- **Performance**: Not designed for performance testing, only functional validation

## Troubleshooting

Common issues:

1. **Missing Chromium**: Install chromium-browser or set `CHROME_BIN_PATH`
2. **Import Errors**: Ensure you're running from the project root
3. **File Cleanup**: Manual cleanup may be needed if test fails unexpectedly
4. **Permission Errors**: Ensure write permissions in project directory

For more detailed debugging, run with verbose output:

```bash
python -m unittest process_report.tests.e2e.test_e2e_pipeline -v
```
