"""
src/stage3_daily_invoice_eligibility_check.py
===============================================
STAGE 3 of the pipeline: Daily Invoice Eligibility Check.

Runs after Stage 2. This is the entry-point validation step that
determines which shops in *today's* new invoice batch are eligible to
receive credit-book financing. Eligibility is defined as:
    - Shop has an 'Approved' KYC status in the platform
    - A credit limit has been assigned (CreditLimit is not null)

Any shop in today's invoice that fails either check is silently dropped.
The surviving, validated invoices are written to branch-specific files that
the credit-operations team uses to disburse loans for that day's invoices.

This stage runs independently from Stage 1 and Stage 2 -- it reads the
same KYC source file (for the approved-shop-with-limit list) and the
branch invoice files from a separate daily-invoices drop folder. It does
NOT depend on the pipeline's output files from earlier stages.
"""

from pathlib import Path

import pandas as pd

from config import (
    EXCLUDED_TEST_SHOP_CODE,
    KYC_SHOPS_PATTERN,
    ONBOARDED_DISTRIBUTORS,
    SOURCE_DIR,
)
from utils.file_utils import get_latest_file
from utils.logging_setup import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Directory layout for Stage 3
# ---------------------------------------------------------------------------
# Invoice files are delivered to a separate folder from the main source
# files used in Stage 1 and Stage 2. Each branch's raw invoice has its
# own sub-directory.
INVOICES_ROOT = Path("data/invoices")

BRANCH_INVOICE_PATTERNS = {
    "HP": ("HP Invoices*.xlsx", INVOICES_ROOT),
    "S1": ("S1 Invoices*.xlsx", INVOICES_ROOT),
    "S3": ("S3 Invoices*.xlsx", INVOICES_ROOT),
    "UPL_KM": ("UPL KM Invoices*.xlsx", INVOICES_ROOT),
}

VALIDATED_INVOICES_OUTPUT_DIR = Path("data/output_files/validated_invoices")

# Expected columns in the HP invoice file (formatted differently from
# S1/S3/UPL_KM, which share a uniform layout -- see load_hp_invoice()).
HP_INVOICE_COLUMNS = [
    "DM", "ShopCode", "VizShopName", "Inv Amount",
    "Previously Paid Amount", "Coins", "UserName", "Password",
]


# ---------------------------------------------------------------------------
# KYC reference data: approved shops with credit limits
# ---------------------------------------------------------------------------
def load_approved_shops_with_limits() -> pd.DataFrame:
    """
    Load KYC data and return only shops with Approved status and a
    non-null credit limit. This is the reference list used to validate
    each branch's invoice file.
    """
    latest_file = get_latest_file(SOURCE_DIR, KYC_SHOPS_PATTERN)
    logger.info("Reading KYC shop data: %s", latest_file)

    kyc = pd.read_excel(latest_file)
    kyc = kyc[kyc["VizShopCode"] != EXCLUDED_TEST_SHOP_CODE]
    kyc = kyc[kyc["DistCenterName"].isin(ONBOARDED_DISTRIBUTORS)]

    approved = kyc[kyc["KYC Status"] == "Approved"][["ShopCode", "CreditLimit"]]
    logger.info("Approved shops with credit limits: %d", len(approved))
    return approved


# ---------------------------------------------------------------------------
# Invoice loading (each branch has slightly different raw-file formatting)
# ---------------------------------------------------------------------------
def load_hp_invoice() -> pd.DataFrame:
    """
    Load the HP branch invoice. The HP file has an extra header row
    (skiprows=1) and an extra leading column (iloc[:, 1:]) not present in
    the other branches -- handled here rather than in generic logic.
    """
    pattern, folder = BRANCH_INVOICE_PATTERNS["HP"]
    latest_file = get_latest_file(folder, pattern)
    logger.info("Reading HP invoice: %s", latest_file)

    df = pd.read_excel(latest_file, skiprows=1)
    df = df.iloc[:, 1:]  # drop the unnamed leading column
    return df[HP_INVOICE_COLUMNS]


def load_branch_invoice(branch_code: str) -> pd.DataFrame:
    """
    Load a branch invoice for branches with a standard file layout
    (S1, S3, UPL_KM).
    """
    pattern, folder = BRANCH_INVOICE_PATTERNS[branch_code]
    latest_file = get_latest_file(folder, pattern)
    logger.info("Reading %s invoice: %s", branch_code, latest_file)
    return pd.read_excel(latest_file)


# ---------------------------------------------------------------------------
# Eligibility validation
# ---------------------------------------------------------------------------
def filter_eligible_invoice_rows(
    invoice_df: pd.DataFrame,
    approved_shops: pd.DataFrame,
    branch_code: str,
) -> pd.DataFrame:
    """
    Validate a branch's invoice against the approved-shop reference list.
    Only rows whose ShopCode has an approved KYC status AND a non-null
    credit limit are kept. Rows for unapproved or limit-less shops are
    dropped -- those shops cannot receive credit-book financing today.

    Parameters
    ----------
    invoice_df : pd.DataFrame
        Raw invoice data for one branch.
    approved_shops : pd.DataFrame
        Approved ShopCodes with CreditLimit from the KYC export.
    branch_code : str
        Branch identifier for logging.

    Returns
    -------
    pd.DataFrame
        Validated invoice rows with CreditLimit column attached.
    """
    total_rows = len(invoice_df)
    merged = invoice_df.merge(approved_shops, on="ShopCode", how="left")
    validated = merged[~merged["CreditLimit"].isnull()].copy()

    dropped = total_rows - len(validated)
    logger.info(
        "%s: %d invoice rows -> %d eligible (dropped %d unapproved/no-limit shops)",
        branch_code, total_rows, len(validated), dropped,
    )
    return validated


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
def write_validated_invoices(branch: str, df: pd.DataFrame) -> None:
    """Write a branch's validated invoice to its own Excel file."""
    VALIDATED_INVOICES_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = VALIDATED_INVOICES_OUTPUT_DIR / f"{branch}_Invoices_Eligible.xlsx"

    mode = "a" if output_path.exists() else "w"
    with pd.ExcelWriter(output_path, mode=mode, engine="openpyxl", if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name="Sheet1", index=False)

    logger.info("%s: validated invoice written -> %s", branch, output_path)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def main() -> None:
    logger.info("Stage 3 started: Daily Invoice Eligibility Check")

    approved_shops = load_approved_shops_with_limits()

    # HP branch (different file format)
    hp_invoices = load_hp_invoice()
    hp_eligible = filter_eligible_invoice_rows(hp_invoices, approved_shops, "HP")
    write_validated_invoices("HP", hp_eligible)

    # Standard-format branches
    for branch_code in ["S1", "S3", "UPL_KM"]:
        raw_invoices = load_branch_invoice(branch_code)
        eligible = filter_eligible_invoice_rows(raw_invoices, approved_shops, branch_code)
        write_validated_invoices(branch_code, eligible)

    logger.info("Stage 3 completed successfully. All branch invoices validated.")


if __name__ == "__main__":
    main()
