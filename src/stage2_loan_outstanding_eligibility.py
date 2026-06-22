"""
src/stage2_loan_outstanding_eligibility.py
============================================
STAGE 2 of the pipeline: Outstanding Loan & Reward-Eligibility Report.

Runs after Stage 1. Re-reads the same two raw sources (repayment
reconciliation + loan data) but applies a narrower, branch-facing set of
calculations: which shops currently have an outstanding balance, how
overdue they are, and whether their most recent repayment behavior
qualifies them for the next round of the reward scheme. Output is split
into one sheet per distributor branch -- this is the file branch managers
actually open day-to-day.

NOTE: this stage intentionally duplicates part of Stage 1's repayment/loan
cleaning rather than importing Stage 1's output. That mirrors the original
production design (two independently-run notebooks against the same raw
exports) and is called out explicitly here rather than silently
"fixed" -- see the README for the suggested next refactor (shared
extraction layer) if this pipeline were rebuilt from scratch.

A few calculations deliberately differ from Stage 1 (different merge key,
simpler overdue formula, narrower reward-qualifying window). These are not
refactor mistakes -- they match the original notebook's business logic
exactly and are commented inline where they diverge.
"""

import pandas as pd

from config import (
    DISTRIBUTOR_A,
    DISTRIBUTOR_B,
    DISTRIBUTOR_C,
    DISTRIBUTOR_D,
    DISTRIBUTOR_E,
    LOAN_DATA_PATTERN,
    MANUALLY_EXCLUDED_LOAN_IDS_FROM_OVERDUE,
    OUTSTANDING_LOAN_REPORT_FILE,
    OUTSTANDING_REPORT_QUALIFYING_WINDOW_DAYS,
    REJECTED_LOANS_FILE,
    REPAYMENT_RECONCILIATION_PATTERN,
    REWARD_OUTSTANDING_THRESHOLD,
    SOURCE_DIR,
)
from utils.file_utils import get_latest_file
from utils.logging_setup import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Repayment reconciliation
# ---------------------------------------------------------------------------
def load_and_clean_repayments() -> pd.DataFrame:
    """Load the latest repayment reconciliation export and keep only successful, de-duplicated rows."""
    latest_file = get_latest_file(SOURCE_DIR, REPAYMENT_RECONCILIATION_PATTERN)
    logger.info("Reading repayment reconciliation file: %s", latest_file)

    df = pd.read_excel(latest_file)
    df = df[df["PaymentTransationStatus"] == "Success"]
    df = df.drop_duplicates(subset="TID")

    df["Loan_Date2"] = pd.to_datetime(df["LoanDate"])
    # Unlike Stage 1, RepaymentDate2 is kept as a full timestamp here (not
    # .dt.date) -- DaysDiff below extracts .dt.date at calculation time
    # instead. Preserved as-is from the original notebook.
    df["RepaymentDate2"] = pd.to_datetime(df["RepaymentDate"], format="mixed")
    return df


def calculate_reward_eligibility(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flag reward-scheme eligibility for the branch-facing report. Uses a
    narrower qualifying window than Stage 1
    (OUTSTANDING_REPORT_QUALIFYING_WINDOW_DAYS vs Stage 1's
    REWARD_QUALIFYING_WINDOW_DAYS) -- matches the original business logic.
    """
    df["DaysDiff"] = df["RepaymentDate2"].dt.date - df["Loan_Date2"].dt.date
    df["DaysDiff"] = pd.to_timedelta(df["DaysDiff"]).dt.days

    df["Qualified"] = "No"
    qualifies = (
        df["DaysDiff"].between(0, OUTSTANDING_REPORT_QUALIFYING_WINDOW_DAYS)
        & (df["Outstanding Amount"] < REWARD_OUTSTANDING_THRESHOLD)
    )
    df.loc[qualifies, "Qualified"] = "Yes"
    return df


def build_repayment_pivot_by_loan_and_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate repayments by Lender_LoanId + RepaymentDate. Note this stage
    merges loans on `Lender_LoanId`, while Stage 1 merges on
    `LoanDisplayId` -- both are valid loan identifiers in the source
    system; preserved exactly as the original notebook used it.
    """
    pivot = pd.pivot_table(
        df,
        values=["Repayment Amount"],
        index=["Lender_LoanId", "RepaymentDate"],
        aggfunc="sum",
        fill_value=0,
    ).reset_index()
    return pivot.ffill()


def build_invoice_wise_repayment(df: pd.DataFrame) -> pd.DataFrame:
    """
    Invoice-level repayment aggregate. Retained for parity with the
    original notebook, which computed this but did not merge it into the
    final branch report in this particular stage.
    """
    pivot = pd.pivot_table(
        df,
        values=["Repayment Amount"],
        index=["Invoice#", "RepaymentDate2", "TID"],
        aggfunc={"Repayment Amount": "sum"},
        fill_value=0,
    ).reset_index()
    pivot = pivot.ffill()
    return pivot[["Invoice#", "Repayment Amount", "RepaymentDate2", "TID"]]


# ---------------------------------------------------------------------------
# Loan data
# ---------------------------------------------------------------------------
def load_and_clean_loans() -> pd.DataFrame:
    """Load the latest loan data export, dropping rejected-then-reapplied and duplicate loans."""
    latest_file = get_latest_file(SOURCE_DIR, LOAN_DATA_PATTERN)
    logger.info("Reading loan data file: %s", latest_file)

    df = pd.read_excel(latest_file)
    df = df[df["TransactionStatus"] == "Paid"]
    df = df.drop_duplicates(subset="Lender_LoanId")

    rejected_loans = pd.read_excel(REJECTED_LOANS_FILE)
    df = df[~df["Lender_LoanId"].isin(rejected_loans["Lender_LoanId"])]

    df["LoanDueDate2"] = pd.to_datetime(df["LoanDueDate"], format="mixed").dt.date
    df["LoanDate2"] = pd.to_datetime(df["LoanDate"]).dt.date
    return df


def merge_loan_with_repayments(loan_df: pd.DataFrame, repayment_pivot: pd.DataFrame) -> pd.DataFrame:
    """Left-join repayment totals onto each loan via Lender_LoanId."""
    merged = loan_df.merge(repayment_pivot, on="Lender_LoanId", how="left")
    merged = merged.fillna(0)
    merged["OutstandingLoan"] = merged["LoanDueAmount"]
    return merged


def calculate_overdue_status(loan_df: pd.DataFrame) -> pd.DataFrame:
    """
    Simple binary overdue flag for the branch report ('Overdue' / '') --
    a coarser version of Stage 1's three-state Over/WithIndue status,
    matching the original notebook's intentionally simpler branch view.
    """
    today = pd.Timestamp.today().date()

    loan_df["OverdueStatus"] = loan_df.apply(
        lambda row: "Overdue"
        if row["OutstandingLoan"] > 0 and today > row["LoanDueDate2"]
        else "",
        axis=1,
    )

    loan_df["No. of Days Overdue"] = 0
    overdue_mask = loan_df["OverdueStatus"] == "Overdue"
    loan_df.loc[overdue_mask, "No. of Days Overdue"] = (
        today - loan_df.loc[overdue_mask, "LoanDueDate2"]
    )
    loan_df = loan_df.fillna(0)

    # Known data-entry exceptions, manually confirmed settled.
    loan_df.loc[
        loan_df["Lender_LoanId"].isin(MANUALLY_EXCLUDED_LOAN_IDS_FROM_OVERDUE),
        "OverdueStatus",
    ] = ""
    loan_df.loc[
        (loan_df["LoanStatus"] == "Rejected") | (loan_df["LoanStatus"] == "Error"),
        "OverdueStatus",
    ] = ""

    loan_df["No. of Days Overdue"] = pd.to_timedelta(
        loan_df["No. of Days Overdue"]
    ).dt.days
    return loan_df


# ---------------------------------------------------------------------------
# Branch-wise disbursement/outstanding report
# ---------------------------------------------------------------------------
def build_branch_outstanding_report(loan_df: pd.DataFrame) -> pd.DataFrame:
    """Build the full outstanding-loan detail table, restricted to loans that still have a balance."""
    pivot = pd.pivot_table(
        loan_df,
        values=["LoanAmount"],
        index=[
            "DistCenterName", "VIZID", "ShopCode", "VizShopName", "Mobile",
            "Inv Date", "Invoice#", "Invoice Amount", "LoanDate2",
            "LoanDueDate2", "LoanDueAmount", "LoanProfit",
            "TotalPayable at the time of repayment", "Repayment Amount",
            "RepaymentDate", "OutstandingLoan", "OverdueStatus",
            "No. of Days Overdue", "DLM",
        ],
        aggfunc="sum",
        fill_value=0,
    ).reset_index().ffill()

    outstanding_only = pivot[pivot["OutstandingLoan"] > 0]
    outstanding_only = outstanding_only.sort_values(by="LoanDate2")

    final_columns = [
        "DistCenterName", "VIZID", "ShopCode", "VizShopName", "Mobile",
        "Inv Date", "Invoice#", "Invoice Amount", "LoanDate2", "LoanDueDate2",
        "LoanAmount", "LoanDueAmount", "LoanProfit",
        "TotalPayable at the time of repayment", "Repayment Amount",
        "RepaymentDate", "OutstandingLoan", "OverdueStatus", "DLM",
    ]
    return outstanding_only[final_columns]


def split_by_distributor(report: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Split the branch-wise outstanding report into one DataFrame per distributor."""
    distributor_to_sheet = {
        DISTRIBUTOR_A: "S1",
        DISTRIBUTOR_B: "S3",
        DISTRIBUTOR_C: "UPLKM",
        DISTRIBUTOR_D: "HP",
        DISTRIBUTOR_E: "Pepsi",
    }
    return {
        sheet_name: report[report["DistCenterName"] == distributor]
        for distributor, sheet_name in distributor_to_sheet.items()
    }


def write_outstanding_report(branch_reports: dict[str, pd.DataFrame]) -> None:
    """Write each distributor's outstanding-loan detail to its own sheet."""
    OUTSTANDING_LOAN_REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if OUTSTANDING_LOAN_REPORT_FILE.exists() else "w"

    with pd.ExcelWriter(OUTSTANDING_LOAN_REPORT_FILE, mode=mode, engine="openpyxl") as writer:
        for sheet_name, branch_df in branch_reports.items():
            branch_df.to_excel(writer, sheet_name=sheet_name, index=True)

    logger.info("Outstanding loan report written: %s", OUTSTANDING_LOAN_REPORT_FILE)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def main() -> None:
    logger.info("Stage 2 started: Outstanding Loan & Reward-Eligibility Report")

    repayments = load_and_clean_repayments()
    repayments = calculate_reward_eligibility(repayments)
    repayment_pivot = build_repayment_pivot_by_loan_and_date(repayments)
    build_invoice_wise_repayment(repayments)  # retained for parity, see docstring

    loans = load_and_clean_loans()
    loans = merge_loan_with_repayments(loans, repayment_pivot)
    loans = calculate_overdue_status(loans)

    report = build_branch_outstanding_report(loans)
    branch_reports = split_by_distributor(report)
    write_outstanding_report(branch_reports)

    logger.info("Stage 2 completed successfully")


if __name__ == "__main__":
    main()
