# Fintech Credit-Risk ETL Pipeline

A production-grade, 3-stage batch ETL pipeline built for a **Buy Now, Pay Later (BNPL) / microfinance lending product** operating across multiple distributor branches. The pipeline ingests raw daily exports from the lending platform, applies a multi-layer KYC/KYB compliance and credit-risk decision engine, and delivers curated reporting tables consumed by a Power BI dashboard used by field operations and credit managers.

---

## Business Context

The platform issues short-duration credit to retail shops at FMCG distributor branches (personal care, beverages, food). Each day, three questions need answers:

1. **Which shops are credit-approved and what is their current loan status?** (Stage 1 — runs weekly/on-demand)
2. **Which shops with active loans qualify for the reward-coins scheme, and which have overdue balances?** (Stage 2 — runs after Stage 1)
3. **Of today's new invoices, which shops are eligible to receive financing?** (Stage 3 — runs daily)

This pipeline answers all three, in sequence.

---

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         RAW DATA SOURCES                            │
│   CreditBook Loan Export │ Repayment Reconciliation │ KYC/KYB Data  │
└────────────┬────────────────────────┬──────────────────────┬────────┘
             │                        │                      │
             ▼                        ▼                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STAGE 1 — Master Loan + Repayment ETL   (stage1_master_...etl.py) │
│                                                                     │
│  ├── Repayment cleaning + Reward/Coins eligibility tagging          │
│  ├── Loan aging + overdue scoring (16-bucket fine-grained)          │
│  ├── Rejected-loan reconciliation (reapplied loans back-merged)     │
│  ├── KYC shop data cleaning + credit-limit utilization              │
│  ├── KYB compliance data ingestion + day-over-day backfill          │
│  ├── Risk categorization (keyword-based from free-text comments)    │
│  └── Multi-step approval decision engine                            │
│         ConfirmedStatus → FinalStatus → KYC Status2                 │
│                                                                     │
│  Output: CreditBook_MasterFile.xlsx  (7 sheets → Power BI)         │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STAGE 2 — Outstanding Loan & Eligibility Report                    │
│            (stage2_loan_outstanding_eligibility.py)                 │
│                                                                     │
│  ├── Re-reads raw exports (independent from Stage 1 output)         │
│  ├── Outstanding balance calculation per shop                       │
│  ├── Binary overdue flag + days overdue                             │
│  └── Branch-wise split (one sheet per distributor)                  │
│                                                                     │
│  Output: OutstandingLoan_BranchWise.xlsx  (one sheet/branch)       │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STAGE 3 — Daily Invoice Eligibility Check                          │
│            (stage3_daily_invoice_eligibility_check.py)              │
│                                                                     │
│  ├── Loads today's invoice files per branch                         │
│  ├── Cross-references against KYC-approved shop + credit-limit list │
│  └── Drops unapproved/no-limit shops; writes validated invoice      │
│                                                                     │
│  Output: {Branch}_Invoices_Eligible.xlsx  (one file per branch)    │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Repository Structure

```
fintech-credit-risk-etl-pipeline/
│
├── config.py                              # All paths, patterns, constants, business-rule thresholds
│
├── utils/
│   ├── file_utils.py                      # Reusable file-discovery helpers (glob + latest-file)
│   └── logging_setup.py                   # Structured logging config shared across all stages
│
├── src/
│   ├── stage1_master_loan_repayment_etl.py
│   ├── stage2_loan_outstanding_eligibility.py
│   └── stage3_daily_invoice_eligibility_check.py
│
├── data/                                  # NOT committed (see .gitignore)
│   ├── source_files/                      # Raw platform exports dropped here before running
│   ├── invoices/                          # Daily branch invoice files (Stage 3 input)
│   └── output_files/                      # Generated reports written here
│
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Key Technical Decisions

### Why 3 separate stages instead of one monolithic script?
Each stage has a different run frequency (Stage 1: weekly, Stage 2: after Stage 1, Stage 3: daily) and different consumers. Keeping them separate means branch managers can run Stage 3 every morning without re-running the heavier Stage 1 KYC/KYB processing.

### Why does Stage 2 re-read raw files instead of consuming Stage 1's output?
This matches the original production design: two independent runs against the same source exports, each producing a self-contained report. The tradeoff (duplicate I/O) was acceptable given the small file sizes. A natural next step if this pipeline were rebuilt would be an extraction layer shared by both stages.

### Approval Decision Engine (Stage 1)
The KYC/KYB approval logic is a two-pass rule engine:

```
Pass 1 (ConfirmedStatus):
  risk_approved + compliance_approved + kyc_approved → "all approved"
  risk_approved + compliance_approved               → "approved by cb, pending vl"
  in risk_rejection_reasons                         → "fully rejected"
  else                                              → "recheck"

Pass 2 (FinalStatus):
  resolves "recheck" into:
    risk_rejected + compliance_approved → "RiskRejected Compliance Approved"
    compliance_rejected                 → "KYC to be reapplied"
    else                                → "No Decision"
```

This two-pass design allowed the operations team to triage "recheck" shops incrementally -- a common pattern in compliance workflows where a single rule can't make a binary call cleanly.

### KYB Day-Over-Day Backfill
The upstream compliance platform occasionally reverts a shop's status to a transitional/processing state during nightly re-scoring. To prevent false negatives in the daily report, the pipeline carries forward the previous run's `current_status` and `risk_assessment_status` whenever today's value isn't `approved` but a prior approved value exists. This is an explicit, documented design choice rather than a silent data patch.

### Reward ("Coins") Qualifying Window
A repayment qualifies for the rewards scheme if it:
- Was made within `N` days of the loan date (configurable in `config.py`)
- Fully cleared the outstanding balance (Outstanding < threshold)

The qualifying window differs between Stage 1 (used for the platform-side rewards report) and Stage 2 (used for the branch-facing outstanding report) -- both values are named constants in `config.py`.

---

## How to Run

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Drop the day's source files into data/source_files/ and branch invoices into data/invoices/

# 3. Run stages in sequence
python -m src.stage1_master_loan_repayment_etl
python -m src.stage2_loan_outstanding_eligibility
python -m src.stage3_daily_invoice_eligibility_check

# Or run a single stage independently
python src/stage3_daily_invoice_eligibility_check.py
```

---

## Data Privacy Note

This is a portfolio version of a production pipeline. All real distributor/partner company names and real loan reference IDs have been replaced with generic placeholders (`Distributor_A`, `Distributor_B`, `EXAMPLE_LOAN_ID_1`, etc.) in `config.py`. Transformation logic, business rules, and data-quality checks are otherwise unchanged from production. No actual PII or proprietary data is present in this repository.

---

## Tech Stack

| Tool | Role |
|---|---|
| Python 3.11+ | Pipeline language |
| pandas | All data transformation and pivot logic |
| openpyxl / xlsxwriter | Multi-sheet Excel output |
| glob / pathlib | File discovery and path management |
| logging | Structured pipeline logging |

---

## Author

**Muhammad Basit Hussain** — Data Engineer / Analytics Engineer  
[GitHub](https://github.com/sadaatalsyed) · [LinkedIn](#)
