"""
config.py
=========
Central configuration for the Fintech Credit-Risk ETL Pipeline.

All paths, file-name patterns, distributor identifiers, and business-rule
thresholds live here so that the pipeline stages contain pure logic and no
hardcoded "magic values". This also makes the pipeline portable across
environments (dev / staging / prod) by changing only this file.

NOTE ON ANONYMIZATION
----------------------
This is a portfolio version of a production pipeline originally built for a
microfinance / "Buy Now Pay Later" lending product. Real distributor /
partner-company names and real loan reference IDs have been replaced with
generic placeholders (Distributor_A, Distributor_B, ...) to protect
confidential business relationships. The transformation logic, business
rules, and data-quality checks are otherwise unchanged from production.
"""

from pathlib import Path

# ---------------------------------------------------------------------------
# Directory layout
# ---------------------------------------------------------------------------
# All raw extracts (Excel exports from the core lending platform) are dropped
# into SOURCE_DIR by an upstream system before this pipeline runs.
SOURCE_DIR = Path("data/source_files")
OUTPUT_DIR = Path("data/output_files")

# ---------------------------------------------------------------------------
# Source file naming patterns
# ---------------------------------------------------------------------------
# The upstream system exports a new timestamped file each run cycle
# (e.g. "CreditBookReconciliationSheet_2026-06-01.xlsx"). The pipeline always
# picks up the most recently created file matching each pattern.
REPAYMENT_RECONCILIATION_PATTERN = "CreditBookReconciliationSheet*.xlsx"
LOAN_DATA_PATTERN = "CreditBookLoanData*.xlsx"
KYC_SHOPS_PATTERN = "CreditBookKYCShopsData*.xlsx"
KYB_STATUS_PATTERN = "PartnerKYBStatus*.xlsx"  # needs latest AND previous file

REJECTED_LOANS_FILE = SOURCE_DIR / "RejectedLoans.xlsx"
REJECTED_LOAN_DETAILS_FILE = SOURCE_DIR / "RejectedLoanDetails.xlsx"
RISK_REJECTED_SHOPS_FILE = SOURCE_DIR / "RiskRejectedShops.xlsx"

# ---------------------------------------------------------------------------
# Output files
# ---------------------------------------------------------------------------
MASTER_FILE = OUTPUT_DIR / "CreditBook_MasterFile.xlsx"
OUTSTANDING_LOAN_REPORT_FILE = OUTPUT_DIR / "OutstandingLoan_BranchWise.xlsx"

# ---------------------------------------------------------------------------
# Distributor / branch identifiers (ANONYMIZED)
# ---------------------------------------------------------------------------
# Original production code referenced real partner-company distribution
# centers by name. They are replaced here with generic codes. Swap in real
# values via environment-specific config when deploying internally.
DISTRIBUTOR_A = "Distributor_A"
DISTRIBUTOR_B = "Distributor_B"
DISTRIBUTOR_C = "Distributor_C"
DISTRIBUTOR_D = "Distributor_D"
DISTRIBUTOR_E = "Distributor_E"
DISTRIBUTOR_F = "Distributor_F"
DISTRIBUTOR_G = "Distributor_G"
DISTRIBUTOR_H = "Distributor_H"
TEST_DISTRIBUTOR = "Test_Distributor_Internal"  # excluded from all reporting

# Distributors that are in-scope for KYC / KYB onboarding checks (Stage 1)
ONBOARDED_DISTRIBUTORS = [
    DISTRIBUTOR_A,
    DISTRIBUTOR_B,
    DISTRIBUTOR_C,
    DISTRIBUTOR_D,
    DISTRIBUTOR_E,
    DISTRIBUTOR_F,
    DISTRIBUTOR_G,
    DISTRIBUTOR_H,
]

# Branch-wise output sheet/file naming used by Stage 2 and Stage 3.
# Maps a distributor identifier -> short branch code used in report sheet
# names and per-branch invoice files.
DISTRIBUTOR_BRANCH_CODE = {
    DISTRIBUTOR_A: "BR1",
    DISTRIBUTOR_B: "BR2",
    DISTRIBUTOR_C: "BR3",
    DISTRIBUTOR_D: "BR4",
    DISTRIBUTOR_E: "BR5",
}

# A known placeholder shop code that should always be excluded (e.g. an
# internal test / sample record seeded by QA).
EXCLUDED_TEST_SHOP_CODE = "TEST-0001"

# ---------------------------------------------------------------------------
# Business-rule constants
# ---------------------------------------------------------------------------
# Reward ("Coins") scheme eligibility: a repayment qualifies for the rewards
# program if it was made within REWARD_QUALIFYING_WINDOW_DAYS of the loan
# date AND the remaining outstanding balance on that transaction is below
# REWARD_OUTSTANDING_THRESHOLD.
REWARD_QUALIFYING_WINDOW_DAYS = 5
REWARD_OUTSTANDING_THRESHOLD = 1
COINS_PER_RUPEE = 20  # reward conversion rate: 1 qualifying rupee = 20 coins

# Stage 2 (outstanding/eligibility report) uses a slightly tighter window
# historically used for the branch-wise outstanding report.
OUTSTANDING_REPORT_QUALIFYING_WINDOW_DAYS = 3

# Loan IDs that are manually excluded from overdue-status flagging because
# they are known data-entry exceptions (e.g. confirmed settled offline but
# not yet closed in the source system). In production this list should be
# sourced from a database table or a maintained exceptions file rather than
# hardcoded — kept here as a config constant so it's at least centralized
# and auditable instead of being buried inside transformation logic.
MANUALLY_EXCLUDED_LOAN_IDS_FROM_OVERDUE = [
    "EXAMPLE_LOAN_ID_1",
    "EXAMPLE_LOAN_ID_2",
    "EXAMPLE_LOAN_ID_3",
]

# Risk-comment keyword -> normalized risk category mapping used in
# Refine_Risk(). Centralized so new keywords can be added without touching
# pipeline logic.
RISK_KEYWORD_MAP = {
    "bad credit": "Bad Credit History",
    "expos": "Over Exposed",       # matches "overexposed" / "exposure"
    "low sales": "Low Sales",
    "cohort": "0",
    "icr": "Poor Rating",
    "rating": "Poor Rating",
}

RISK_REJECTION_REASONS = ["Over Exposed", "Bad Credit History", "Poor Rating"]
