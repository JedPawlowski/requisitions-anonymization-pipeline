import pandas as pd
import numpy as np
from faker import Faker
import re

# ============================================================
# Configuration & reproducibility
# ============================================================

fake = Faker()
Faker.seed(42)
np.random.seed(42)

INPUT_FILE = "requisitions_raw.xlsx"
OUTPUT_FILE = "requisitions_anonymized.xlsx"

print("Python script started")

# -------------------------------------------------
# Load job reference dimension
# -------------------------------------------------

job_ref = pd.read_excel("job_reference_table.xlsx")
job_ref.columns = job_ref.columns.str.strip()

print("Job reference table loaded")
print("Reference rows:", len(job_ref))

# -------------------------------------------------
# Build lookup dictionaries
# -------------------------------------------------

TITLE_TO_FAMILY = dict(zip(job_ref["Job Posting Title"], job_ref["Job Family"]))
TITLE_TO_GRADE = dict(zip(job_ref["Job Posting Title"], job_ref["Job Grade"]))
TITLE_TO_PROGRAMME = dict(zip(job_ref["Job Posting Title"], job_ref["Programme Type"]))

# ============================================================
# Load Excel (non-standard header handled explicitly)
# ============================================================

df = pd.read_excel(INPUT_FILE, header=3)
df.columns = df.columns.str.strip()

# -------------------------------------------------
# Load geo reference dimension
# -------------------------------------------------

geo_ref = pd.read_csv(
    "geo_reference.csv",
    encoding="utf-8-sig",
    header=0
)

# If everything collapsed into one column, split it
if len(geo_ref.columns) == 1:
    geo_ref = geo_ref.iloc[:, 0].astype(str).str.split(",", expand=True)
    geo_ref.columns = ["country", "region", "city", "currency"]

# Normalize column names (after split!)
geo_ref.columns = geo_ref.columns.str.strip().str.lower()

print("geo_ref columns:", geo_ref.columns.tolist())

print("Excel loaded")
print(f"Rows: {len(df)}")
print(f"Columns: {len(df.columns)}")

# Work on a copy (raw data remains untouched)
df_anon = df.copy()

# --- String & text helpers ---

def normalize_name(value):
    if pd.isna(value):
        return None
    return str(value).strip()

def scrub_string(value):
    if isinstance(value, str):
        return re.sub(r"medtronic", "", value, flags=re.IGNORECASE)
    return value

# --- Dimension anonymization helpers ---

def anonymize_org_dimension(series, prefix):
    values = series.dropna().unique()
    mapping = {val: f"{prefix}_{i+1:03d}" for i, val in enumerate(values)}
    return series.map(mapping)

# --- Business derivation helpers ---

def derive_workflow_name(row):
    if row["Programme Type"] == "Internship":
        return "Internship"
    if row["Programme Type"] == "Graduate Programme":
        return "Graduate / Early Career"
    if row["Job Grade"] in ["Executive", "VIP"]:
        return "Executive"
    if row["Job Grade"] in ["Leadership", "Management"]:
        return "Management"
    if pd.notna(row["Number of Openings Total"]) and row["Number of Openings Total"] >= 5:
        return "Volume"
    return "Professional"

# Randomly sample a realistic city / county / currency per country
def derive_geo_fields(country):
    subset = geo_ref[geo_ref["country"] == country]

    if subset.empty:
        return pd.Series(["Unknown City", "Unknown County", None])

    row = subset.sample(1).iloc[0]

    return pd.Series([
        row["city"],
        row["region"],
        row["currency"]
    ])

# ============================================================
# Anonymize primary identifiers
# ============================================================

req_ids = df_anon["Job Requisition ID"].astype(str).unique()
req_id_map = {old: f"REQ_{i:06d}" for i, old in enumerate(req_ids, start=1)}

df_anon["Job Requisition ID"] = df_anon["Job Requisition ID"].map(req_id_map)

print("Job Requisition IDs anonymized")

# ============================================================
# Anonymize people (names & emails)
# ============================================================

people_cols = [
    "Hiring Manager",
    "Recruiter",
    "Primary Recruiter",
    "Primary Sourcer",
    "Recruiters as of Most Recent Fill Date"
]

for col in people_cols:
    df_anon[col] = df_anon[col].apply(normalize_name)

unique_people = set()
for col in people_cols:
    unique_people.update(df_anon[col].dropna().unique())

people_map = {p: fake.name() for p in unique_people}

def map_name(value):
    return people_map.get(value) if pd.notna(value) else value

for col in people_cols:
    df_anon[col] = df_anon[col].map(map_name)

print(f"People anonymized ({len(unique_people)} unique individuals)")

# ============================================================
# Operating Structure anonymization
# ============================================================

if "Operating Structure" in df_anon.columns:
    df_anon["Operating Structure"] = anonymize_org_dimension(
        df_anon["Operating Structure"], "ORG"
    )

print("Operating Structure anonymized")

# ============================================================
# Cost centers anonymization
# ============================================================

if "Cost Centre" in df_anon.columns:
    df_anon["Cost Centre"] = df_anon["Cost Centre"].apply(
        lambda x: (
            "".join(c for c in str(x).strip() if c.isdigit())
            if pd.notna(x) and str(x).strip()[0].isdigit()
            else None
        )
    )

print("Cost centers anonymized")

# ============================================================
# Job Posting Title anonymization
# ============================================================

VALID_TITLES = job_ref["Job Posting Title"].unique().tolist()

unique_titles = df_anon["Job Posting Title"].dropna().unique()
title_mapping = {
    real: VALID_TITLES[i % len(VALID_TITLES)]
    for i, real in enumerate(unique_titles)
}

df_anon["Job Posting Title"] = df_anon["Job Posting Title"].map(title_mapping)

# -------------------------------------------------
# Derive job-related attributes from reference
# -------------------------------------------------

df_anon["Job Family"] = df_anon["Job Posting Title"].map(TITLE_TO_FAMILY)
df_anon["Job Grade"] = df_anon["Job Posting Title"].map(TITLE_TO_GRADE)
df_anon["Programme Type"] = df_anon["Job Posting Title"].map(TITLE_TO_PROGRAMME)

df_anon["Job Desc"] = None

print("Job attributes derived from reference table")

missing_refs = df_anon[df_anon["Job Family"].isna()]

if not missing_refs.empty:
    print("WARNING: Missing job reference for some titles")
    print(missing_refs["Job Posting Title"].unique())

# -------------------------------------------------
# Derive salary ranges
# -------------------------------------------------

GRADE_TO_SALARY_RANGE = {
    "Early Career": (2000, 4000),
    "IC": (4000, 7000),
    "Professional": (5000, 9000),
    "Manager": (7000, 12000),
    "Director": (10000, 16000),
    "Senior Director": (14000, 20000),
    "Executive": (18000, 30000)
}

# Generate realistic but non-identifiable salary ranges per job grade
def generate_salary(job_grade):
    low, high = GRADE_TO_SALARY_RANGE.get(job_grade, (3000, 6000))
    min_sal = np.random.randint(low, high - 1000)
    max_sal = min_sal + np.random.randint(1000, 4000)
    return pd.Series([min_sal, max_sal])

df_anon[["Expected Salary Min", "Expected Salary Max"]] = (
    df_anon["Job Grade"].apply(generate_salary)
)

# -------------------------------------------------
# Derive geo attributes from reference
# -------------------------------------------------

df_anon[["City", "County", "Currency"]] = (
    df_anon["Country"].apply(derive_geo_fields)
)

df_anon["Advertising Country"] = df_anon["Country"]

# ============================================================
# Dates (single global offset)
# ============================================================

date_cols = [
    "Date Request Entered",
    "Recruiting Start Date",
    "Request Completed Date",
    "Target Hire Date",
    "Job Requisition Fill Date",
    "Close Date",
    "Earliest Job Posting Start Date"
]

offset_days = np.random.randint(180, 365)
print(f"Date offset applied: {offset_days} days")

for col in date_cols:
    if col in df_anon.columns:
        df_anon[col] = pd.to_datetime(df_anon[col], errors="coerce") + pd.Timedelta(days=offset_days)

# Created By HM should be based on Request Completed Date
if "Request Completed Date" in df_anon.columns:
    df_anon["Created By HM"] = df_anon["Request Completed Date"]

df_anon["Approved Date"] = (
    df_anon["Recruiting Start Date"]
    - pd.to_timedelta(np.random.randint(5, 15, size=len(df_anon)), unit="D")
)

mask = np.random.rand(len(df_anon)) < 0.7
df_anon.loc[mask, "External Posting Date"] = (
    df_anon.loc[mask, "Approved Date"]
    + pd.to_timedelta(np.random.randint(1, 10, size=mask.sum()), unit="D")
)

df_anon["Internal Careers Posting"] = (
    df_anon["Approved Date"]
    + pd.to_timedelta(np.random.randint(0, 5, size=len(df_anon)), unit="D")
)

df_anon["Posting Date"] = df_anon[
    ["External Posting Date", "Internal Careers Posting"]
].min(axis=1)

df_anon["Transaction Date"] = df_anon["Posting Date"]

df_anon["Campaign Year"] = df_anon["Posting Date"].dt.year

df_anon["Contract End Date"] = (
    df_anon["Target Hire Date"]
    + pd.to_timedelta(np.random.randint(180, 720, size=len(df_anon)), unit="D")
)

# Intake Meeting happens shortly before Recruiting Start Date
df_anon["Intake Meeting"] = (
    df_anon["Recruiting Start Date"]
    - pd.to_timedelta(
        np.random.randint(1, 7, size=len(df_anon)),
        unit="D"
    )
)

# ============================================================
# Business logic derivations
# ============================================================

# Workflow depends on Job Grade, Programme Type, and Number of Openings
df_anon["Workflow Name"] = df_anon.apply(derive_workflow_name, axis=1)

df_anon["Posting Agency"] = np.where(
    df_anon["External Posting Date"].notna(),
    "Yes",
    "No"
)

df_anon["Internal Only Requisition"] = np.where(
    df_anon["External Posting Date"].notna(),
    "No",
    "Yes"
)

df_anon["Publish To Agencies"] = np.where(
    df_anon["Posting Agency"] == "Yes", "Yes", "No"
)

df_anon["Posting Type (Int/Ext)"] = np.where(
    df_anon["External Posting Date"].notna(),
    "External",
    "Internal"
)

ALL_RECRUITERS = df_anon["Recruiter"].dropna().unique()

def pick_secondary(row):
    if len(ALL_RECRUITERS) <= 1:
        return None
    return np.random.choice([r for r in ALL_RECRUITERS if r != row["Recruiter"]])

df_anon["Secondary Recruiter"] = df_anon.apply(pick_secondary, axis=1)

if "Is Evergreen" in df_anon.columns:
    df_anon["Pipeline (Evergreen)"] = (
        df_anon["Is Evergreen"]
        .fillna("No")
        .replace("", "No")
    )
else:
    df_anon["Pipeline (Evergreen)"] = "No"

# ============================================================
# Free-text cleanup & final defensive scrub
# ============================================================

text_cols = [
    "Recruiting Instruction",
    "Close Comments",
    "Justification",
    "Pending Role Assignment (for Open/Frozen Job Requisitions)"
]

for col in text_cols:
    if col in df_anon.columns:
        df_anon[col] = None

for col in df_anon.select_dtypes(include="object"):
    df_anon[col] = df_anon[col].apply(scrub_string)

print("Free-text cleared and global scrub applied")

# ============================================================
# Requisitions schema – final column selection & renaming
# ============================================================

REQUISITION_COLUMNS_MAP = {
    "Workflow Name": "Workflow Name",
    "Country": "Country",
    "Job Requisition ID": "Requisition ID",
    "Posting Agency": "Posting Agency",
    "City": "City",
    "Job Posting Title": "Final Job Title",
    "Advertising Country": "Advertising Country",
    "Approved Date": "Approved Date",
    "Recruiter": "ATS Recruiter",
    "Primary Sourcer": "Sourcer",
    "Justification": "Business Justification",
    "Close Date": "Closed Date",
    "Contract End Date": "Contract End Date",
    "County": "County",
    "Created By HM": "Created By HM",
    "Currency": "Currency",
    "Expected Salary Max": "Expected Salary Max",
    "Expected Salary Min": "Expected Salary Min",
    "Target Hire Date": "Expected Start Date",
    "External Posting Date": "External Posting Date",
    "Intake Meeting": "Intake Meeting",
    "Internal Only Requisition": "Internal Only Requisition",
    "Internal Careers Posting": "Internal Careers Posting",
    "Job Family": "Job Family",
    "Job Desc": "Job Desc",
    "Job Grade": "Job Grade",
    "Number of Openings Total": "No Positions Total",
    "Operating Structure": "Org Level",
    "Pipeline (Evergreen)": "Pipeline (Evergreen)",
    "Posting Date": "Posting Date",
    "Publish To Agencies": "Publish To Agencies",
    "Secondary Recruiter": "Secondary Recruiter",
    "Worker Type Hiring Requirement": "Type of Position",
    "Transaction Date": "Transaction Date",
    "Posting Type (Int/Ext)": "Posting Type (Int/Ext)",
    "Recruiting Start Date": "Recruitment start date",
    "Job Requisition Status": "Current Req Status",
    "Programme Type": "Programme Type",
    "Campaign Year": "Campaign Year"
}

# Keep only required columns that actually exist
existing_columns = {
    src: tgt
    for src, tgt in REQUISITION_COLUMNS_MAP.items()
    if src in df_anon.columns
}

df_anon = df_anon[list(existing_columns.keys())].rename(columns=existing_columns)

print("Final Requisitions schema applied")
print("Columns:", list(df_anon.columns))

# ============================================================
# Save output
# ============================================================

df_anon.to_excel(OUTPUT_FILE, index=False)
print(f"Anonymized file saved: {OUTPUT_FILE}")
