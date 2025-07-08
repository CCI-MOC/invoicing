import os
import tempfile
from pathlib import Path
import pandas as pd
import pytest
import shutil
import logging
import subprocess


# Test Configuration
PIPELINE_TIMEOUT = 600  # 10 minutes
INVOICE_MONTH = "2025-06"
CHROME_BIN_PATH = "/usr/bin/chromium"

# Expected output files from the pipeline
EXPECTED_CSV_FILES = [
    "billable 2025-06.csv",
    "nonbillable 2025-06.csv",
    "NERC-2025-06-Total-Invoice.csv",
    "BU_Internal 2025-06.csv",
    "Lenovo 2025-06.csv",
    "MOCA-A_Prepaid_Groups-2025-06-Invoice.csv",
    "NERC_Prepaid_Group-Credits-2025-06.csv",
]

EXPECTED_DIRECTORIES = ["pi_invoices"]


@pytest.fixture
def project_root():
    """Get the root directory of the project."""
    return Path(__file__).parent.parent.parent.parent


@pytest.fixture
def test_data_dir():
    """Get the directory containing test data files."""
    return Path(__file__).parent / "test_data"


@pytest.fixture
def test_workspace(test_data_dir, project_root):
    """Create a temporary workspace with test data and pipeline code."""
    with tempfile.TemporaryDirectory() as temp_dir:
        workspace = Path(temp_dir)
        test_files = _setup_workspace(test_data_dir, project_root, workspace)
        yield workspace, test_files


def _setup_workspace(test_data_dir, project_root, workspace):
    """Set up the workspace by copying test data and pipeline code."""
    # Copy test data files
    test_files = {}
    for test_file in test_data_dir.glob("*"):
        dest_path = workspace / test_file.name
        shutil.copy(test_file, dest_path)
        test_files[test_file.name] = dest_path

    # Copy pipeline code
    process_report_src = project_root / "process_report"
    process_report_dest = workspace / "process_report"
    shutil.copytree(process_report_src, process_report_dest)

    return test_files


def _prepare_pipeline_execution(test_files, workspace):
    """Build command and environment for pipeline execution."""
    # Build command
    command = [
        "python",
        "-m",
        "process_report.process_report",
        "--invoice-month",
        INVOICE_MONTH,
        "--pi-file",
        str(test_files["test_pi.txt"]),
        "--projects-file",
        str(test_files["test_projects.txt"]),
        "--timed-projects-file",
        str(test_files["test_timed_projects.txt"]),
        "--BU-subsidy-amount",
        "100",
        "--old-pi-file",
        str(test_files["test_PI.csv"]),
        "--alias-file",
        str(test_files["test_alias.csv"]),
        "--prepay-debits",
        str(test_files["test_prepay_debits.csv"]),
        "--prepay-credits",
        str(test_files["test_prepay_credits.csv"]),
        "--prepay-projects",
        str(test_files["test_prepay_projects.csv"]),
        "--coldfront-data-file",
        str(test_files["test_coldfront_api_data.json"]),
        str(test_files["test_nerc-ocp-test 2025-04.csv"]),
        str(test_files["test_NERC OpenShift 2025-04.csv"]),
        "--prepay-contacts",
        str(test_files["test_prepay_contacts.csv"]),
    ]

    # Set up environment
    env = os.environ.copy()
    env["CHROME_BIN_PATH"] = CHROME_BIN_PATH
    env["PYTHONPATH"] = str(workspace) + ":" + env.get("PYTHONPATH", "")

    return command, env


def _run_pipeline(command, env, workspace):
    """Run the pipeline and return the result."""
    logger = logging.getLogger(__name__)
    logger.info(f"Running pipeline in: {workspace}")

    try:
        result = subprocess.run(
            command,
            env=env,
            cwd=workspace,
            capture_output=True,
            text=True,
            timeout=PIPELINE_TIMEOUT,
        )

        if result.stderr:
            logger.warning(f"Pipeline stderr: {result.stderr}")

        return result

    except subprocess.TimeoutExpired:
        pytest.fail(f"Pipeline execution timed out after {PIPELINE_TIMEOUT} seconds")


def _validate_outputs(workspace):
    """Validate all expected pipeline outputs."""
    logger = logging.getLogger(__name__)
    logger.info(f"Validating pipeline outputs in: {workspace}")

    # Validate CSV files
    for csv_file in EXPECTED_CSV_FILES:
        csv_path = workspace / csv_file
        assert csv_path.exists(), f"CSV file not found: {csv_path}"
        assert csv_path.is_file(), f"Path is not a file: {csv_path}"
        assert csv_path.stat().st_size > 0, f"CSV file is empty: {csv_path}"

        # Check file can be read as CSV
        try:
            df = pd.read_csv(csv_path)
            assert len(df.columns) > 0, f"CSV has no columns: {csv_path}"
        except Exception as e:
            pytest.fail(f"Failed to read CSV {csv_path}: {e}")

    # Validate PI invoices directory
    pi_dir = workspace / "pi_invoices"
    assert pi_dir.exists(), f"PI invoices directory not found: {pi_dir}"
    assert pi_dir.is_dir(), f"PI invoices path is not a directory: {pi_dir}"

    pdf_files = list(pi_dir.glob("*.pdf"))
    assert len(pdf_files) > 0, f"No PDF files found in {pi_dir}"

    logger.info("All pipeline outputs validated successfully")


def test_e2e_pipeline_execution(test_workspace):
    """
    End-to-end test of the entire invoice processing pipeline.

    This test:
    1. Sets up a temporary workspace with test data
    2. Runs the complete pipeline with test inputs
    3. Validates that all expected outputs are generated
    4. Checks that output files have correct structure
    """
    workspace, test_files = test_workspace

    # Prepare pipeline execution
    command, env = _prepare_pipeline_execution(test_files, workspace)

    # Run the pipeline
    result = _run_pipeline(command, env, workspace)

    # Check pipeline succeeded
    assert result.returncode == 0, (
        f"Pipeline failed with exit code {result.returncode}\n"
        f"Stdout: {result.stdout}\n"
        f"Stderr: {result.stderr}"
    )

    # Validate outputs
    _validate_outputs(workspace)
