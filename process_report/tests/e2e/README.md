# End-to-End (E2E) Tests

This directory contains end-to-end tests for the NERC invoicing pipeline. These tests are designed to validate that the entire pipeline runs successfully from start to finish with realistic data.

## Purpose

The E2E tests are **not** intended to cover all edge cases or detailed validation - that's what unit tests are for. Instead, they:

1. **Validate Pipeline Execution**: Ensure the full pipeline runs without errors as a subprocess
2. **Check Output Generation**: Verify that all expected output files are generated correctly
3. **Structural Validation**: Perform basic sanity checks on output file structures
4. **Integration Testing**: Test that all pipeline components work together correctly

## What the E2E Test Covers

The main E2E test (`test_e2e_pipeline.py`) validates the complete pipeline execution by:

### Pipeline Execution
- Runs the pipeline as a subprocess with realistic test data
- Tests with invoice month "2025-06"
- Uses a 10-minute timeout for complete pipeline execution
- Validates successful execution (exit code 0)

### Output Validation
- Verifies generation of all expected CSV files:
  - `billable 2025-06.csv`
  - `nonbillable 2025-06.csv`
  - `NERC-2025-06-Total-Invoice.csv`
  - `BU_Internal 2025-06.csv`
  - `Lenovo 2025-06.csv`
  - `MOCA-A_Prepaid_Groups-2025-06-Invoice.csv`
  - `NERC_Prepaid_Group-Credits-2025-06.csv`
  - `OCP_TEST 2025-06.csv`
- Validates PDF generation in the `pi_invoices/` directory
- Ensures no unexpected files are created
- Performs basic structural validation (non-empty files, readable CSVs)

## Test Data

The `test_data/` directory contains the minimal dataset required by the pipeline:

- **Invoice Files**: `test_nerc-ocp-test 2025-04.csv`, `test_NERC OpenShift 2025-04.csv`
- **Configuration Files**:
  - PI lists: `test_pi.txt`
  - Project lists: `test_projects.txt`, `test_timed_projects.txt`
  - Prepay configurations: `test_prepay_debits.csv`, `test_prepay_credits.csv`, `test_prepay_projects.csv`, `test_prepay_contacts.csv`
- **Historical Data**: `test_PI.csv` (old PI file), `test_alias.csv`
- **API Data**: `test_coldfront_api_data.json`

## Running the Tests

### Prerequisites

1. Install dependencies in your environment:

When in the project root folder:

   ```bash
   pip install -r requirements.txt
   pip install -r process_report/tests/test-requirements.txt
   ```

2. Install Chromium (for PDF generation):
   ```bash
   # Ubuntu/Debian
   sudo apt-get install chromium-browser

   # macOS
   brew install chromium
   ```

### Environment Variables

The test sets these automatically, but you can override if needed:

```bash
export CHROME_BIN_PATH=/usr/bin/chromium
export PYTHONPATH=/path/to/project/root
```

### Local Execution

Run the E2E tests locally using pytest:

```bash
# From the project root directory
python -m pytest process_report/tests/e2e/

# Or run the specific test file
python -m pytest process_report/tests/e2e/test_e2e_pipeline.py

# Run with verbose output
python -m pytest process_report/tests/e2e/ -v --log-cli-level=INFO
```

## CI/CD Integration

The E2E tests run automatically in GitHub Actions:

- **Triggers**: On push to main, pull requests, and manual dispatch
- **Environment**: Ubuntu with Python 3.12 and Chromium
- **Timeout**: 10 minutes for complete pipeline execution
- **Artifacts**: Test outputs are uploaded on failure for debugging

## Test Output and Cleanup

The test:
1. Creates all output files in a temporary workspace
2. Uses symlinks to required configuration files (institute_list.yaml, templates/)
3. Validates file existence, structure, and content
4. Automatic cleanup handled by pytest's tmp_path fixture

If the test fails, check:
- The error message and stack trace
- Pipeline stdout/stderr output
- Generated output files (uploaded as artifacts in CI)

## Extending the Tests

To add new validations:

1. **Output Files**: Add to `EXPECTED_CSV_FILES` list
2. **Expected Directories**: Add to `EXPECTED_DIRECTORIES` list
3. **Test Data**: Add new files to `test_data/` directory
4. **Pipeline Arguments**: Modify command building in `_prepare_pipeline_execution()`
5. **Validation Logic**: Extend `_validate_outputs()` function

## Limitations

- **No S3 Integration**: S3 operations are bypassed in this e2e test implementation.
- **External APIs**: Coldfront and other external APIs are also bypassed in this e2e testing implementation.
- **Non-Representative Test Data**: the test data in the `test_data/` represents only the minimum required data to run the pipeline.

## Troubleshooting

Common issues:

1. **Missing Chromium**: Install chromium-browser or set `CHROME_BIN_PATH`
2. **Import Errors**: Ensure you're running from the project root
3. **File Cleanup**: Manual cleanup may be needed if test fails unexpectedly
4. **Permission Errors**: Ensure write permissions in project directory

For more detailed debugging, run with verbose output:

```bash
python -m pytest process_report/tests/e2e/test_e2e_pipeline.py -v --log-cli-level=INFO
```
