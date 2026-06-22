"""
src/stage1_master_loan_repayment_etl.py
========================================
STAGE 1 of the pipeline: Master Loan + Repayment ETL.

Reads the latest raw exports (repayment reconciliation, loan data, KYC
shop data, KYB compliance/risk data), applies the full set of business
rules used by the lending platform, and writes a curated multi-sheet
"Master File" that downstream BI tooling (Power BI) consumes directly.

Responsibilities covered in this stage:
    1. Repayment reconciliation cleaning + reward ("Coins") eligibility
    2. Loan data cleaning, merge with repayments, aging/overdue scoring
    3. Disbursement detail reporting (branch-wise)
    4. Rejected-loan reconciliation (re-attaching internally approved loans)
    5. KYC shop data cleaning + credit-limit utilization
    6. KYB compliance/risk data ingestion with day-over-day backfill
    7. Risk categorization + multi-step approval-status decision engine
    8. Master KYC/credit summary + final multi-sheet Excel load

This is a direct, function-by-function refactor of the original analyst
notebook (ETL_Code-Main.ipynb) -- every transformation step has been
preserved. What changed is structure only: each pandas operation is now a
named, documented, independently testable function instead of a flat
sequence of notebook cells, with print() replaced by structured logging
and magic values moved to config.py.
"""

from pathlib import Path

import numpy as np
import pandas as pd

from config import (
    DISTRIBUTOR_BRANCH_CODE,
    EXCLUDED_TEST_SHOP_CODE,
    KYB_STATUS_PATTERN,
    KYC_SHOPS_PATTERN,
    LOAN_DATA_PATTERN,
    MANUALLY_EXCLUDED_LOAN_IDS_FROM_OVERDUE,
    MASTER_FILE,
    ONBOARDED_DISTRIBUTORS,
    REJECTED_LOANS_FILE,
    REJECTED_LOAN_DETAILS_FILE,
    REPAYMENT_RECONCILIATION_PATTERN,
    REWARD_OUTSTANDING_THRESHOLD,
    REWARD_QUALIFYING_WINDOW_DAYS,
    COINS_PER_RUPEE,
    RISK_KEYWORD_MAP,
    RISK_REJECTED_SHOPS_FILE,
    RISK_REJECTION_REASONS,
    SOURCE_DIR,
    TEST_DISTRIBUTOR,
)
from utils.file_utils import get_latest_file, get_latest_two_files
from utils.logging_setup import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# 1. Repayment reconciliation + reward ("Coins") eligibility
# ---------------------------------------------------------------------------
def load_repayment_reconciliation_data() -> pd.DataFrame:
    """Load the most recent repayment-reconciliation export."""
    latest_file = get_latest_file(SOURCE_DIR, REPAYMENT_RECONCILIATION_PATTERN)
    logger.info("Reading repayment reconciliation file: %s", latest_file)
    return pd.read_excel(latest_file)


def clean_repayment_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only successful, non-test, de-duplicated repayment transactions
    and normalize the loan / repayment date columns.
    """
    df = df[df["PaymentTransationStatus"] == "Success"].copy()
    df = df[df["DistCenterName"] != TEST_DISTRIBUTOR]
    df["UserId2"] = df["UserID"]
    df = df.drop_duplicates(subset="TID")

    df["Loan_Date2"] = pd.to_datetime(df["LoanDate"])
    df["RepaymentDate2"] = pd.to_datetime(df["RepaymentDate"], format="mixed").dt.date
    return df


def calculate_reward_eligibility(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flag each repayment as 'Qualified' (Yes/No) for the reward/Coins scheme:
    a repayment qualifies if it landed within REWARD_QUALIFYING_WINDOW_DAYS
    of the loan date AND fully cleared the balance (Outstanding Amount
    below REWARD_OUTSTANDING_THRESHOLD).
    """
    df["DaysDiff"] = df["RepaymentDate2"] - df["Loan_Date2"].dt.date
    df["DaysDiff"] = pd.to_timedelta(df["DaysDiff"]).dt.days

    df["Qualified"] = "No"
    qualifies = (
        df["DaysDiff"].between(0, REWARD_QUALIFYING_WINDOW_DAYS)
        & (df["Outstanding Amount"] < REWARD_OUTSTANDING_THRESHOLD)
    )
    df.loc[qualifies, "Qualified"] = "Yes"
    logger.info("Repayments qualifying for reward scheme: %d", qualifies.sum())
    return df


def build_loan_id_repayment_pivots(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build two loan-level repayment aggregates:
      - by LoanDisplayId only (used to merge onto the loan master record)
      - by LoanDisplayId + RepaymentDate2 + TID (used for rejected-loan
        reconciliation, where multiple repayments per loan must stay
        distinguishable)
    """
    by_loan = pd.pivot_table(
        df,
        values=["Repayment Amount"],
        index=["LoanDisplayId"],
        aggfunc={"Repayment Amount": "sum"},
        fill_value=0,
    ).reset_index()
    by_loan = by_loan.ffill()

    by_loan_date_tid = pd.pivot_table(
        df,
        values=["Repayment Amount"],
        index=["LoanDisplayId", "RepaymentDate2", "TID"],
        aggfunc={"Repayment Amount": "sum"},
        fill_value=0,
    ).reset_index()
    by_loan_date_tid = by_loan_date_tid.ffill()

    return by_loan, by_loan_date_tid


def _move_column_before(df: pd.DataFrame, column_to_move: str, target_column: str) -> pd.DataFrame:
    """Reorder `df` so `column_to_move` sits immediately before `target_column`."""
    target_idx = df.columns.get_loc(target_column)
    columns = list(df.columns)
    columns.remove(column_to_move)
    columns.insert(target_idx, column_to_move)
    return df[columns]


def build_repayment_coins_pivot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build the transaction-level reward report: for every qualifying
    repayment, calculate the rupee reward amount and the coin equivalent.
    """
    pivot = pd.pivot_table(
        df,
        values=["Repayment Amount"],
        index=[
            "DistCenterName", "VIZID", "ShopCode", "VizShopName", "Inv Date",
            "Invoice#", "Invoice Amount", "Loan_Date2", "LoanAmount",
            "LoanDisplayId", "LoanProfit", "Total Payable", "LoanDueDate",
            "RepaymentDate2", "Outstanding Amount", "PaymentTransationStatus",
            "DaysDiff", "Qualified",
        ],
        aggfunc={"Repayment Amount": "sum"},
        fill_value=0,
    ).reset_index()
    pivot = pivot.ffill()
    pivot = _move_column_before(pivot, "Repayment Amount", "Outstanding Amount")
    pivot = pd.DataFrame(pivot)

    is_qualified = pivot["Qualified"] == "Yes"
    pivot["Coins Amount  (In Rupees)"] = (
        pivot["Repayment Amount"] - pivot["LoanAmount"]
    ).where(is_qualified, "")
    pivot["Number of Coins"] = pivot["Coins Amount  (In Rupees)"] * COINS_PER_RUPEE
    pivot["DisbursementStatus"] = np.where(is_qualified, "Disbursed", "")

    return pivot


def build_invoice_wise_repayment(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate repayments by invoice -- used later for rejected-loan reconciliation."""
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
# 2. Loan data cleaning + aging/overdue scoring
# ---------------------------------------------------------------------------
def load_loan_data() -> pd.DataFrame:
    """Load the most recent loan-data export."""
    latest_file = get_latest_file(SOURCE_DIR, LOAN_DATA_PATTERN)
    logger.info("Reading loan data file: %s", latest_file)
    return pd.read_excel(latest_file)


def clean_loan_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop loans that were rejected-then-reapplied (tracked in a separate
    exceptions file), keep only paid/settled transactions on real
    distributors, and de-duplicate by loan ID.
    """
    rejected_loans = pd.read_excel(REJECTED_LOANS_FILE)
    df = df[~df["Lender_LoanId"].isin(rejected_loans["Lender_LoanId"])]

    df = df[df["TransactionStatus"] == "Paid"]
    df = df[df["DistCenterName"] != TEST_DISTRIBUTOR]
    df = df.drop_duplicates(subset="Lender_LoanId")

    df["LoanDueDate2"] = pd.to_datetime(df["LoanDueDate"], format="mixed").dt.date
    df["LoanDate2"] = pd.to_datetime(df["LoanDate"]).dt.date
    return df


def merge_loan_with_repayments(loan_df: pd.DataFrame, repayment_by_loan: pd.DataFrame) -> pd.DataFrame:
    """Left-join repayment totals onto each loan (equivalent to an Excel VLOOKUP)."""
    merged = loan_df.merge(repayment_by_loan, on="LoanDisplayId", how="left")
    merged = merged.fillna(0)
    merged["OutstandingLoan"] = merged["LoanDueAmount"]
    return merged


def calculate_overdue_status(row: pd.Series) -> str:
    """
    Coarse-grained payment status for a single loan: fully paid, still
    within its due date, or overdue.
    """
    outstanding = row["OutstandingLoan"]
    days = row["No. of Days"]
    if outstanding <= 0:
        return "LoanPaid"
    elif outstanding > 0 and days <= 0:
        return "Within due-date"
    elif outstanding > 0 and days > 0:
        return "Overdue"


def calculate_aging_status(row: pd.Series) -> str:
    """
    Fine-grained aging bucket for an outstanding loan, used for the
    overdue-aging dashboard (e.g. "Due Tomorrow", "Overdue by 1-2 months").
    """
    outstanding = row["OutstandingLoan"]
    days = row["No. of Days"]

    if outstanding <= 0:
        return ""

    if days <= 0:
        if days == 0:
            return "Due Today"
        elif days == -1:
            return "Due Tomorrow"
        elif days == -2:
            return "Due Day after Tomorrow"
        elif -7 <= days <= -3:
            return "Due in 3-7 days"
        elif -14 <= days < -7:
            return "Due in 1-2 weeks"
        elif -21 <= days < -14:
            return "Due in 2-3 weeks"
        else:
            return "Due in 3+ weeks"

    # days > 0 (overdue)
    if days == 1:
        return "Overdue by 1 day"
    elif days == 2:
        return "Overdue by 2 days"
    elif 3 <= days <= 7:
        return "Overdue by 3-7 days"
    elif 7 < days <= 14:
        return "Overdue by 1-2 weeks"
    elif 14 < days <= 21:
        return "Overdue by 2-3 weeks"
    elif 21 < days <= 30:
        return "Overdue by 3-4 weeks"
    elif 30 < days <= 60:
        return "Overdue by 1-2 months"
    elif days > 60:
        return "Overdue by 2+ months"


def apply_aging_and_overdue(loan_df: pd.DataFrame) -> pd.DataFrame:
    """Attach 'No. of Days', coarse overdue status, and fine-grained aging bucket."""
    today = pd.Timestamp.today().date()
    loan_df["No. of Days"] = today - loan_df["LoanDueDate2"]
    loan_df = loan_df.fillna(0)
    loan_df["No. of Days"] = pd.to_timedelta(loan_df["No. of Days"]).dt.days

    loan_df["Over/WithIndue"] = loan_df.apply(calculate_overdue_status, axis=1)
    loan_df.loc[loan_df["LoanStatus"] == "Rejected", "Over/WithIndue"] = (
        "Overdue but Cash Collection"
    )

    loan_df["Aging Status"] = loan_df.apply(calculate_aging_status, axis=1)

    # Known data-entry exceptions, manually confirmed settled -- excluded
    # from overdue flagging. See config.MANUALLY_EXCLUDED_LOAN_IDS_FROM_OVERDUE.
    loan_df.loc[
        loan_df["Lender_LoanId"].isin(MANUALLY_EXCLUDED_LOAN_IDS_FROM_OVERDUE),
        "Over/WithIndue",
    ] = ""

    return loan_df


def build_disbursement_details(loan_df: pd.DataFrame) -> pd.DataFrame:
    """Branch-wise disbursement + repayment + aging detail report."""
    pivot = pd.pivot_table(
        loan_df,
        values=["LoanAmount"],
        index=[
            "DistCenterName", "VIZID", "ShopCode", "VizShopName", "Mobile",
            "Inv Date", "Invoice#", "Invoice Amount", "LoanDate2",
            "LoanDueDate2", "LoanDisplayId", "LoanStatus", "LoanDueAmount",
            "LoanProfit", "TotalPayable at the time of repayment",
            "Repayment Amount", "OutstandingLoan", "No. of Days",
            "Over/WithIndue", "Aging Status", "DLM",
        ],
        aggfunc="sum",
        fill_value=0,
    ).reset_index()
    pivot = pivot.ffill()
    pivot = pivot[pivot["DistCenterName"] != TEST_DISTRIBUTOR]

    final_columns = [
        "DistCenterName", "VIZID", "ShopCode", "VizShopName", "Mobile",
        "Inv Date", "Invoice#", "Invoice Amount", "LoanDate2", "LoanDueDate2",
        "LoanDisplayId", "LoanStatus", "LoanAmount", "LoanDueAmount",
        "LoanProfit", "TotalPayable at the time of repayment",
        "Repayment Amount", "OutstandingLoan", "No. of Days", "Over/WithIndue",
        "Aging Status", "DLM",
    ]
    return pivot[final_columns]


def build_viz_id_pivots(loan_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Per-shop loan totals, used downstream to compute credit-limit
    utilization (Stage 1 KYC step) and the master KYC summary.
    """
    loan_amount_only = pd.pivot_table(
        loan_df, values=["LoanAmount"], index=["VIZID"], aggfunc="sum", fill_value=0
    ).reset_index().ffill()

    full_detail = pd.pivot_table(
        loan_df,
        values=["LoanAmount", "Repayment Amount", "LoanDueAmount"],
        index=["VIZID"],
        aggfunc="sum",
        fill_value=0,
    ).reset_index().ffill()

    return loan_amount_only, full_detail


# ---------------------------------------------------------------------------
# 3. Rejected-loan reconciliation
# ---------------------------------------------------------------------------
def build_rejected_loan_report(
    loan_df: pd.DataFrame, repayment_by_loan_date_tid: pd.DataFrame
) -> pd.DataFrame:
    """
    Append any loan that was internally approved *after* the last known
    rejected-loan snapshot back into the rejected-loan tracking sheet (so
    operations has full visibility on every loan, not just the rejected
    ones), then attach repayment status per loan.
    """
    rejected_loan_details = pd.read_excel(REJECTED_LOAN_DETAILS_FILE)
    max_date_in_rejected_loans = max(pd.to_datetime(rejected_loan_details["LoanDate"]))

    internally_approved_loans = loan_df[
        (loan_df["LoanDate2"] > max_date_in_rejected_loans.date())
        & (loan_df["LoanStatus"] == 0)
    ]

    # Columns that already exist on the loan master record.
    shared_columns = [
        "DistCenterName", "VIZID", "ShopCode", "VizShopName", "Mobile", "DLM",
        "Inv Date", "Invoice#", "Invoice Amount", "LoanDisplayId", "LoanDate",
        "LoanAmount", "LoanProfit", "CreditDuration",
    ]
    # Columns only present in the rejected-loan tracking sheet -- back-filled
    # with sensible defaults for loans that were never actually rejected.
    tracking_only_columns = [
        "TransactionStatus", "Reason", "TimeDifference", "LoanStatus.1",
        "RepaymentStatus", "To Be Reconcile By", "Status",
    ]

    rearranged = pd.DataFrame()
    rearranged[shared_columns] = internally_approved_loans[shared_columns]
    rearranged[tracking_only_columns] = 0
    rearranged.loc[:, "TransactionStatus"] = "Paid"
    rearranged.loc[:, ["Reason", "TimeDifference", "RepaymentStatus"]] = " "
    rearranged.loc[:, "LoanStatus.1"] = "Internal Approved"
    rearranged.loc[:, "To Be Reconcile By"] = "Internal Ops"
    rearranged.loc[:, "Status"] = "No"

    combined = pd.concat([rejected_loan_details, rearranged], ignore_index=True)
    combined = combined.merge(repayment_by_loan_date_tid, on="LoanDisplayId", how="left")
    combined.loc[combined["TID"].isnull(), "Status"] = "No"
    combined.loc[combined["TID"].notnull(), "Status"] = "Yes"

    return combined


# ---------------------------------------------------------------------------
# 4. KYC shop data + credit-limit utilization
# ---------------------------------------------------------------------------
def load_kyc_data() -> pd.DataFrame:
    """Load the most recent KYC (Know Your Customer) shop-onboarding export."""
    latest_file = get_latest_file(SOURCE_DIR, KYC_SHOPS_PATTERN)
    logger.info("Reading KYC shop data file: %s", latest_file)
    return pd.read_excel(latest_file)


def clean_kyc_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep only in-scope distributors, drop a known placeholder shop record,
    and resolve shops with conflicting KYC statuses by dropping the stale
    'Pending' row in favor of the finalized one.
    """
    df = df[df["DistCenterName"].isin(ONBOARDED_DISTRIBUTORS)]
    df = df[df["VizShopCode"] != EXCLUDED_TEST_SHOP_CODE]

    status_counts = df.groupby(["ShopCode"])["KYC Status"].nunique()
    shops_with_conflicting_status = status_counts[status_counts > 1].index
    df = df[
        ~(
            df["ShopCode"].isin(shops_with_conflicting_status)
            & (df["KYC Status"] == "Pending")
        )
    ]
    return df


def calculate_credit_limit_usage(kyc_df: pd.DataFrame, loan_amount_by_viz_id: pd.DataFrame) -> pd.DataFrame:
    """Attach each shop's used credit (sum of its loans) and remaining limit."""
    merged = kyc_df.merge(
        loan_amount_by_viz_id, left_on="VizShopCode", right_on="VIZID", how="left"
    )
    merged = merged.rename(columns={"LoanAmount": "LimitUsed"})
    merged["Remaining Limit"] = merged["CreditLimit"] - merged["LimitUsed"]
    return merged


# ---------------------------------------------------------------------------
# 5. KYB compliance/risk data + risk categorization + approval decisioning
# ---------------------------------------------------------------------------
def load_kyb_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load the latest and previous KYB (Know Your Business) compliance exports."""
    latest_path, previous_path = get_latest_two_files(SOURCE_DIR, KYB_STATUS_PATTERN)
    logger.info("Reading latest KYB file: %s", latest_path)
    logger.info("Reading previous KYB file: %s", previous_path)

    columns = [
        "external_customer_id", "current_status", "compliance_reason",
        "risk_assessment_status", "risk_assessment_coment",
    ]
    latest = pd.read_excel(latest_path)[columns].drop_duplicates(subset="external_customer_id")
    previous = pd.read_excel(previous_path)[columns].drop_duplicates(subset="external_customer_id")
    return latest, previous


def backfill_kyb_status(latest: pd.DataFrame, previous: pd.DataFrame) -> pd.DataFrame:
    """
    Day-over-day backfill: if today's compliance/risk status isn't
    'approved' but yesterday's snapshot had a value for that business,
    carry yesterday's value forward. This smooths over transient
    re-processing states in the upstream compliance system.
    """
    combined = latest.merge(
        previous, on="external_customer_id", how="left", suffixes=("", "_old")
    )

    has_previous_value = combined["current_status_old"].notna()

    not_approved_mask = (combined["current_status"] != "approved") & has_previous_value
    combined.loc[not_approved_mask, "current_status"] = combined.loc[
        not_approved_mask, "current_status_old"
    ]

    risk_not_approved_mask = (
        combined["risk_assessment_status"] != "approved"
    ) & has_previous_value
    combined.loc[risk_not_approved_mask, "risk_assessment_status"] = combined.loc[
        risk_not_approved_mask, "risk_assessment_status_old"
    ]

    return combined


def refine_risk_category(row: pd.Series) -> str:
    """
    Normalize the free-text risk-assessment comment into a fixed set of
    risk categories via keyword matching (see config.RISK_KEYWORD_MAP).
    """
    comment = str(row["risk_assessment_coment"]).lower()
    for keyword, category in RISK_KEYWORD_MAP.items():
        if keyword in comment:
            return category
    return "0"


def merge_kyc_kyb(kyc_df: pd.DataFrame, kyb_df: pd.DataFrame) -> pd.DataFrame:
    """Attach KYB compliance/risk data onto each KYC shop record."""
    merged = kyc_df.merge(
        kyb_df, left_on="CreatedBy", right_on="external_customer_id", how="left"
    )
    merged["risk_assessment_coment"] = merged["risk_assessment_coment"].astype(str)
    merged["Risk_Refined"] = merged.apply(refine_risk_category, axis=1)

    # Helper flags used elsewhere in reporting to identify shops with a
    # non-unique VizShopCode (duplicate onboarding records).
    is_duplicate_code = merged["VizShopCode"].duplicated()
    merged["Unique VizId formula"] = merged["VizShopCode"].where(~is_duplicate_code, "mm")
    merged["Unique VizId"] = merged["VizShopCode"].where(~is_duplicate_code, "")

    return merged


def attach_risk_rejected_shops(kyc_df: pd.DataFrame) -> pd.DataFrame:
    """Attach the manually maintained list of shops rejected by the risk team."""
    risk_rejected_shops = pd.read_excel(RISK_REJECTED_SHOPS_FILE)
    merged = kyc_df.merge(risk_rejected_shops, on="VizShopCode", how="left")
    merged["FullyRejected"] = merged["Risk_Refined"].apply(
        lambda risk: "full rejected" if risk in RISK_REJECTION_REASONS else ""
    )
    return merged


def determine_confirmed_status(row: pd.Series) -> str:
    """
    First-pass approval decision combining the platform's own risk
    assessment, the compliance (KYB) status, and the original KYC status.
    """
    risk_status = row["risk_assessment_status"]
    compliance_status = row["current_status"]
    fully_rejected = row["FullyRejected"]
    kyc_status = row["KYC Status"].lower()

    if risk_status == "approved" and compliance_status == "approved" and kyc_status == "approved":
        return "all approved"
    elif risk_status == "approved" and compliance_status == "approved":
        return "approved by cb pending in vl"
    elif fully_rejected == "full rejected":
        return "fully rejected"
    else:
        return "recheck"


def determine_final_status(row: pd.Series) -> str:
    """
    Second-pass decision that resolves the 'recheck' bucket from
    determine_confirmed_status() into an actionable final status.
    """
    confirmed_status = row["ConfirmedStatus"]
    risk_status = row["risk_assessment_status"]
    compliance_status = row["current_status"]

    if confirmed_status == "all approved":
        return "all approved"
    elif confirmed_status == "approved by cb pending in vl":
        return "approved by cb pending in vl"
    elif confirmed_status == "fully rejected":
        return "fully rejected"
    elif risk_status == "rejected" and compliance_status == "approved":
        return "RiskRejected Compliance Approved"
    elif risk_status in ("rejected", "approved") and compliance_status == "rejected":
        return "KYC to be reapplied"
    else:
        return "No Decision"


def map_kyc_status_from_final_status(row: pd.Series) -> str:
    """Translate the internal FinalStatus decision into the customer-facing KYC Status label."""
    final_status = row["FinalStatus"].lower()
    if final_status == "fully rejected":
        return "Rejected"
    elif final_status == "kyc to be reapplied":
        return "Need more info"
    elif final_status == "all approved":
        return "Approved"
    else:
        return "Pending"


def apply_approval_decision_engine(kyc_df: pd.DataFrame) -> pd.DataFrame:
    """Run the full 3-step KYC/KYB approval decisioning pipeline."""
    kyc_df["current_status"] = kyc_df["current_status"].dropna().apply(str)

    kyc_df["ConfirmedStatus"] = kyc_df.apply(determine_confirmed_status, axis=1)
    kyc_df["FinalStatus"] = kyc_df.apply(determine_final_status, axis=1)
    kyc_df["FinalStatus2"] = kyc_df["FinalStatus"]
    kyc_df["KYC Status2"] = kyc_df.apply(map_kyc_status_from_final_status, axis=1)
    kyc_df = kyc_df.fillna(0)
    return kyc_df


# ---------------------------------------------------------------------------
# 6. Master KYC / credit summary
# ---------------------------------------------------------------------------
def build_kyc_master_summary(kyc_df: pd.DataFrame, loan_detail_by_viz_id: pd.DataFrame) -> pd.DataFrame:
    """Build the consolidated per-shop KYC + credit-utilization summary table."""
    pivot = pd.pivot_table(
        kyc_df,
        values=["Remaining Limit"],
        index=[
            "DistCenterName", "VizShopCode", "ShopCode", "KYC Business Name",
            "KYC Shopkeeper NTN", "KYC Contact Number", "KYC Status2",
            "Reason Phrase", "Contract Status", "CreatedDate", "current_status",
            "compliance_reason", "risk_assessment_status", "risk_assessment_coment",
            "CreditLimit", "LimitUsed",
        ],
        aggfunc="sum",
        fill_value=0,
    ).reset_index().ffill()

    pivot = pivot.merge(
        loan_detail_by_viz_id, left_on="VizShopCode", right_on="VIZID", how="left"
    )

    final_columns = [
        "DistCenterName", "VizShopCode", "ShopCode", "KYC Business Name",
        "KYC Shopkeeper NTN", "KYC Contact Number", "KYC Status2",
        "Contract Status", "Reason Phrase", "CreatedDate", "current_status",
        "compliance_reason", "risk_assessment_status", "risk_assessment_coment",
        "CreditLimit", "LimitUsed", "Remaining Limit", "LoanAmount",
        "Repayment Amount", "LoanDueAmount",
    ]
    return pivot[final_columns]


def build_kyc_status_summary_by_distributor(kyc_df: pd.DataFrame) -> pd.DataFrame:
    """Count of shops per distributor broken down by FinalStatus -- a quick health check view."""
    pivot = pd.pivot_table(
        kyc_df,
        values=["Unique VizId"],
        index=["DistCenterName"],
        columns=["FinalStatus"],
        aggfunc="count",
        fill_value=0,
    ).reset_index().ffill()
    return pivot


# ---------------------------------------------------------------------------
# 7. Output
# ---------------------------------------------------------------------------
def write_master_file(
    kyc_master_summary: pd.DataFrame,
    rejected_loan_report: pd.DataFrame,
    kyc_data: pd.DataFrame,
    repayment_data: pd.DataFrame,
    repayment_coins: pd.DataFrame,
    loan_data: pd.DataFrame,
    disbursement_details: pd.DataFrame,
) -> None:
    """Write every curated table into the multi-sheet Master File consumed by Power BI."""
    MASTER_FILE.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if MASTER_FILE.exists() else "w"

    with pd.ExcelWriter(MASTER_FILE, mode=mode, engine="openpyxl") as writer:
        kyc_master_summary.to_excel(writer, sheet_name="KYCs_Master", index=False)
        rejected_loan_report.to_excel(writer, sheet_name="RejectedLoanDetails", index=True)
        kyc_data.to_excel(writer, sheet_name="KYCsData", index=False)
        repayment_data.to_excel(writer, sheet_name="RepaymentData", index=True)
        repayment_coins.to_excel(writer, sheet_name="RepaymentCoins", index=True)
        loan_data.to_excel(writer, sheet_name="LoanData", index=True)
        disbursement_details.to_excel(writer, sheet_name="DisbursementDetails", index=True)

    logger.info("Master File written: %s", MASTER_FILE)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def main() -> None:
    logger.info("Stage 1 started: Master Loan + Repayment ETL")

    # -- Repayments --
    repayments = load_repayment_reconciliation_data()
    repayments = clean_repayment_data(repayments)
    repayments = calculate_reward_eligibility(repayments)
    repayment_by_loan, repayment_by_loan_date_tid = build_loan_id_repayment_pivots(repayments)
    repayment_coins = build_repayment_coins_pivot(repayments)
    build_invoice_wise_repayment(repayments)  # retained for parity / downstream reuse

    # -- Loans --
    loans = load_loan_data()
    loans = clean_loan_data(loans)
    loans = merge_loan_with_repayments(loans, repayment_by_loan)
    loans = apply_aging_and_overdue(loans)
    disbursement_details = build_disbursement_details(loans)
    loan_amount_by_viz_id, loan_detail_by_viz_id = build_viz_id_pivots(loans)

    # -- Rejected-loan reconciliation --
    rejected_loan_report = build_rejected_loan_report(loans, repayment_by_loan_date_tid)

    # -- KYC --
    kyc = load_kyc_data()
    kyc = clean_kyc_data(kyc)
    kyc = calculate_credit_limit_usage(kyc, loan_amount_by_viz_id)

    # -- KYB + risk decisioning --
    kyb_latest, kyb_previous = load_kyb_data()
    kyb = backfill_kyb_status(kyb_latest, kyb_previous)
    kyc = merge_kyc_kyb(kyc, kyb)
    kyc = attach_risk_rejected_shops(kyc)
    kyc = apply_approval_decision_engine(kyc)

    # -- Master summaries --
    kyc_master_summary = build_kyc_master_summary(kyc, loan_detail_by_viz_id)
    build_kyc_status_summary_by_distributor(kyc)  # quick-glance health-check pivot

    kyc_data_columns = [
        "LenderName", "VizShopCode", "DistCenterName", "ShopCode",
        "Viz shopkeeper ContactNumber", "KYC Shopkeeper NTN",
        "Viz shopkeeper CNIC", "KYC Contact Number", "vizlink shopkeeper name",
        "KYC Shopkeeper Name", "viz Business name", "KYC Business Name",
        "Viz Shop Address", "KYC Shop Address", "KYC Status", "Contract Status",
        "Reason Phrase", "CreatedBy", "CreatedDate", "KYC_Month", "KYC_Day",
        "JsonObject", "KYC_Approved", "Contract_Approved", "CreditLimit",
        "VizLinkCreditLimit", "Unique VizId formula", "Unique VizId",
        "LimitUsed", "Remaining Limit", "current_status", "compliance_reason",
        "risk_assessment_status", "risk_assessment_coment", "Risk_Refined",
        "FullyRejected", "ConfirmedStatus", "FinalStatus", "FinalStatus2",
        "KYC Status2",
    ]
    kyc_for_export = kyc.drop(columns=["VIZID", "external_customer_id"])[kyc_data_columns]

    write_master_file(
        kyc_master_summary=kyc_master_summary,
        rejected_loan_report=rejected_loan_report,
        kyc_data=kyc_for_export,
        repayment_data=repayments,
        repayment_coins=repayment_coins,
        loan_data=loans,
        disbursement_details=disbursement_details,
    )

    logger.info("Stage 1 completed successfully")


if __name__ == "__main__":
    main()
