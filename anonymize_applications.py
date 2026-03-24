"""
Applications anonymization - SYNTHETIC DATASET
Purpose: Generate anonymized + synthetic Applications data
Scope: Demo / mock Power BI dashboards only
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import hashlib

CLIENTS = [
    {"code": "C1", "name": "Client 1", "share": 1.0},
    {"code": "C2", "name": "Client 2", "share": 0.8},
    {"code": "C3", "name": "Client 3", "share": 0.3},
    {"code": "C4", "name": "Client 4", "share": 0.6},
]

pd.options.mode.copy_on_write = True

MAX_DATE = pd.Timestamp("2026-01-31")

HEADER_ROW = 2

SOURCE_REF_PATH = "mappings/application_source_reference.xlsx"

source_reference = pd.read_excel(SOURCE_REF_PATH)

source_reference = (
    source_reference[["Source", "Source Medium"]]
    .dropna()
    .reset_index(drop=True)
)

RELIGION_VALUES = [
    "Christian",
    "Muslim",
    "Hindu",
    "Buddhist",
    "Jewish",
    "No religion",
    "Prefer not to say",
    "Other",
]

DISABILITY_VALUES = [
    "No",
    "Yes",
    "Prefer not to say",
]

DEGREE_VALUES = [
    "Bachelor of Science",
    "Bachelor of Arts",
    "Master of Science",
    "Master of Arts",
    "MBA",
    "PhD",
    "No Degree",
]

FINAL_COLUMN_MAP = {
        # IDs (created by mapping merge)
        "Application Ref": "Application Ref",
        "Candidate Ref": "Candidate Ref",

        # Core
        "Job Application Date": "Applied Date",
        "Candidate Stage": "Candidate status",
        "Hired": "Hired",

        # Source
        "Internal": "Is Internal",

        # Location
        "Candidate City": "Address City",
        "Candidate Country": "Address Country",
        "Candidate State": "State",

        # Dates
        "Candidate Start Date": "Start Date",

        # Sensitive Data
        "Ethnicity": "Ethnicity",
        "Gender": "Gender",

        }

def assign_source_from_reference(app_ref: str, ref_df: pd.DataFrame):
    idx = stable_hash(app_ref) % len(ref_df)
    row = ref_df.iloc[idx]
    return row["Source"], row["Source Medium"]

def assign_requisition(app_ref: str, req_df: pd.DataFrame):
    idx = stable_hash(app_ref) % len(req_df)
    row = req_df.iloc[idx]
    return (
        row["Requisition ID"],
        row["Final Job Title"],
        row["ATS Recruiter"],
        row["Expected Salary Max"],
    )

def assign_from_enum(key: str, values: list[str]) -> str:
    """
    Deterministically assign a categorical value from a fixed enum.
    """
    return values[stable_hash(key) % len(values)]

# IMPORTANT:
# Built-in Python hash() is randomized between interpreter sessions.
# Using md5-based hashing guarantees deterministic synthetic data generation.
def stable_hash(key: str) -> int:
    """
    Stable deterministic hash for synthetic data generation.
    Python built-in hash() changes between runs — this does not.
    """
    return int(hashlib.md5(str(key).encode()).hexdigest(), 16)

def synthetic_offset_days(series, min_days=1, max_days=5):

    """
    Generate deterministic day offsets based on string values.
    """
    return (
        series
        .astype(str)
        .apply(lambda x: (stable_hash(x) % (max_days - min_days + 1)) + min_days)
    )

def funnel_speed_multiplier(app_ref):
    """
    Creates realistic funnel pacing:
    20% fast-track
    60% normal
    20% slow
    """
    v = stable_hash(app_ref) % 100

    if v < 20:
        return 0.6   # fast hires
    elif v < 80:
        return 1.0   # normal
    else:
        return 1.6   # slow process

def funnel_seniority_multiplier(seniority):
    """
    Enterprise behavior:
    junior hires move fast,
    exec hires move slow.
    """
    if seniority == "junior":
        return 0.7
    elif seniority == "mid":
        return 1.0
    elif seniority == "senior":
        return 1.3
    else:
        return 1.6

def synthetic_step_date(
    base_date: pd.Series,
    key: pd.Series,
    min_days: int,
    max_days: int,
    cap_date: pd.Timestamp,
    seniority=None,
):
    """
    Generate a deterministic synthetic date offset from base_date.
    """
    if max_days < min_days:
        raise ValueError("max_days must be >= min_days")

    offsets = key.apply(lambda x: (stable_hash(x) % (max_days - min_days + 1)) + min_days)

    # candidate pacing
    speed = key.apply(funnel_speed_multiplier)

    offsets = offsets * speed

    # seniority pacing
    if seniority is not None:
        seniority_weight = seniority.apply(funnel_seniority_multiplier)

        # Align explicitly
        seniority_weight = seniority_weight.reindex(offsets.index)

        offsets = offsets * seniority_weight

    offsets = pd.to_numeric(offsets, errors="coerce")
    
    offsets = offsets.fillna(0).round().astype(int)

    result = base_date + pd.to_timedelta(offsets, unit="D")

    return result.clip(upper=cap_date)

def hiring_wave_shift(dates):
    """
    Enterprise hiring cycles:
    Spring & Autumn hiring spikes,
    Summer slowdown.
    """
    m = dates.dt.month

    return (
        (m.isin([4,5,6]) * 4)   # spring hiring push
        + (m.isin([9,10,11]) * 6)  # autumn peak
        - (m.isin([7,8]) * 3)   # summer slowdown
    )

# ============================================================
# SENIORITY INFERENCE
# Enterprise-style role banding based on salary
# ============================================================

def infer_seniority(max_salary):
    try:
        s = float(max_salary)
    except Exception:
        return "mid"

    if s < 50000:
        return "junior"
    elif s < 90000:
        return "mid"
    elif s < 130000:
        return "senior"
    else:
        return "exec"


print(">>> anonymize_applications.py started <<<")


# ---- configuration ----

RAW_INPUT_PATH = Path("applications/applications_raw.csv")

MAPPING_DIR = Path("mappings")
APPLICATION_ID_MAP_PATH = MAPPING_DIR / "application_id_map.csv"
CANDIDATE_ID_MAP_PATH = MAPPING_DIR / "candidate_id_map.csv"

CHUNK_SIZE = 100_000

# ---- chunk reader ----

def read_applications_in_chunks(path: Path):
    return pd.read_csv(
        path,
        header=HEADER_ROW,
        chunksize=CHUNK_SIZE,
        low_memory=False
    )

def load_mappings():
    application_map = pd.read_csv(APPLICATION_ID_MAP_PATH)
    candidate_map = pd.read_csv(CANDIDATE_ID_MAP_PATH)

    return application_map, candidate_map


# final column order
FINAL_OUTPUT_COLUMNS = [
    "Company Name",
    "Client Code",
    "Application Ref",
    "Candidate Ref",

    # Requisition
    "Requisition ID",
    "Job Title",
    "Recruiter at Time of Fill",
    "Max Salary",

    # Dates
    "Application Initiated Date",
    "Applied Date",
    "Application/CV Review Date",
    "Sourcer Screen Date",
    "Recruiter Interview Date",
    "CV submitted to Hiring Manager date",
    "Hiring Manager Review Date",
    "Hiring Manager Review Completed Date",
    "Hiring Manager Interview Date",
    "Video Interview Date",
    "2nd Interview Date",
    "Online Assessment/Test Date",
    "Assessment Centre Date",
    "Offer Made Date",
    "Written Offer Made Date",
    "Written Offer Accepted Date",
    "Offer Accepted Date",
    "Verbal Offer Made Date",
    "Verbal Offer Accepted Date",
    "Security/Background Check Date",
    "Ready to Hire Date",
    "Start Date",
    "Withdrawal Date",
    "Rejection Date",
    "Candidate Reneged Date",

    # Candidate attributes (synthetic)
    "Ethnicity",
    "Gender",
    "Religion",
    "Indicated Disability Status",
    "Degree Name",
    "Age / DOB",
    "Education Level",
    "Graduated",
    "Graduation Date",
    "Institution",
    "Nationality",
    "Military Service",
    "Notice Period",
    "Reasonable Adjustments",


    # Other
    "Application Reason For Withdrawal",
    "Application Reason For Rejection",
    "Candidate status",
    "Is Internal",
    "Hired",
    "Hired Country",
    "Source Medium",
    "Source",
    "Source Name",
    "City",
    "Address City",
    "Address Country",
    "State",
    "Candidate Reneged",
    "Contract Type",
    "Hours per Week",
]


# ---- main process ----

def process(client_code, client_name, share):

    processed_rows = 0

    OUTPUT_PATH = Path(f"applications/{client_code}_applications.csv")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # --- Ensure clean regeneration (avoid accidental append) ---
    if OUTPUT_PATH.exists():
        OUTPUT_PATH.unlink()

    REQUISITIONS_PATH = Path(f"requisitions/{client_code}_requisitions.xlsx")

    requisitions_df = pd.read_excel(
        REQUISITIONS_PATH,
        usecols=[
            "Requisition ID",
            "Final Job Title",
            "ATS Recruiter",
            "Expected Salary Max",
        ]
    )

    # Defensive checks
    assert not requisitions_df.empty
    assert requisitions_df["Requisition ID"].is_unique

    TOTAL_APPLICATIONS = sum(
    len(chunk)
    for chunk in read_applications_in_chunks(RAW_INPUT_PATH)
    )

    target_applications = int(TOTAL_APPLICATIONS * share)

    # Create deterministic client-specific sampling mask
    def include_row(app_id):
        return (stable_hash(client_code + "_" + str(app_id)) % 1000) < int(share * 1000)

    print(f"Target applications for {client_code}: {target_applications}")
    print("Requisitions count:", len(requisitions_df))
    print("Raw file rows:", TOTAL_APPLICATIONS)

    print("Starting Applications anonymization (chunked)...")

    application_map, candidate_map = load_mappings()

    for i, chunk in enumerate(read_applications_in_chunks(RAW_INPUT_PATH)):

        if processed_rows >= target_applications:
            break

        # ---- process chunk normally ----
        chunk["Job Application ID"] = chunk["Job Application ID"].astype(str)
        chunk["Candidate ID"] = chunk["Candidate ID"].astype(str)

        # --- Deterministic client sampling ---
        chunk = chunk[
            chunk["Job Application ID"].apply(include_row)
        ]

        if chunk.empty:
            continue
        
        processed_chunk = (
            chunk
            .merge(application_map, on="Job Application ID", how="left")
            .merge(candidate_map, on="Candidate ID", how="left")
        )

        processed_chunk["Application Ref"] = (
            client_code + "_" + processed_chunk["Application Ref"].astype(str)
        )

        processed_chunk["Candidate Ref"] = (
            client_code + "_" + processed_chunk["Candidate Ref"].astype(str)
        )


        # Fail fast if mapping missing

        if processed_chunk["Application Ref"].isna().any():
            raise ValueError("Missing Application Ref mapping detected")

        if processed_chunk["Candidate Ref"].isna().any():
            raise ValueError("Missing Candidate Ref mapping detected")

        processed_chunk = processed_chunk[list(FINAL_COLUMN_MAP.keys())]
        processed_chunk = processed_chunk.rename(columns=FINAL_COLUMN_MAP)

        print(f"Processing chunk {i + 1}")

# ============================================================
# STAGE 1 — CORE NORMALIZATION
# - ID mapping
# - canonical applied date
# - requisition linkage
# - seniority inference
# ============================================================


        # === CREATE CANONICAL APPLIED DATE HELPER ===
        processed_chunk["_applied_dt"] = pd.to_datetime(
            processed_chunk["Applied Date"],
            dayfirst=True,
            errors="coerce"
        )

        if i == 0:
            assert pd.api.types.is_datetime64_any_dtype(processed_chunk["_applied_dt"])


        # --- Assign requisitions to applications ---
        assigned_reqs = processed_chunk["Application Ref"].apply(
            lambda x: assign_requisition(x, requisitions_df)
        )

        processed_chunk["Requisition ID"] = assigned_reqs.apply(lambda x: x[0])
        processed_chunk["Job Title"] = assigned_reqs.apply(lambda x: x[1])
        processed_chunk["Recruiter at Time of Fill"] = assigned_reqs.apply(lambda x: x[2])
        processed_chunk["Max Salary"] = assigned_reqs.apply(lambda x: x[3])

        processed_chunk["_seniority"] = processed_chunk["Max Salary"].apply(infer_seniority)


        # === GLOBAL DATE NORMALIZATION ===

        latest_applied = processed_chunk["_applied_dt"].max()

        # Step 1 — align newest application to MAX_DATE
        if pd.notna(latest_applied):
            global_shift_days = (MAX_DATE - latest_applied).days
        else:
            global_shift_days = 0

        processed_chunk["_applied_dt"] = (
            processed_chunk["_applied_dt"]
            + pd.to_timedelta(global_shift_days, unit="D")
        )

        # Step 2 — create historical enterprise spread
        historical_spread = synthetic_offset_days(
            processed_chunk["Application Ref"],
            min_days=0,
            max_days=720   # ~2 years back
        )

        processed_chunk["_applied_dt"] = (
            processed_chunk["_applied_dt"]
            - pd.to_timedelta(historical_spread, unit="D")
        )

        processed_chunk["_wave_shift"] = hiring_wave_shift(processed_chunk["_applied_dt"])

        processed_chunk["_applied_dt"] = (
            processed_chunk["_applied_dt"]
            + pd.to_timedelta(processed_chunk["_wave_shift"], unit="D")
        )

        processed_chunk["_applied_dt"] = processed_chunk["_applied_dt"].clip(upper=MAX_DATE)

        assert "_applied_dt" in processed_chunk.columns

        processed_chunk["_end_dt"] = MAX_DATE


        processed_chunk["Hired"] = (
            processed_chunk["Hired"]
            .astype(str)
            .str.strip()
            .str.lower()
            .isin(["true", "yes", "y", "1"])
        )

        processed_chunk["Candidate status"] = (
            processed_chunk["Candidate status"]
            .astype(str)
            .str.strip()
            .str.lower()
        )


# ============================================================
# STAGE 2 — ENTERPRISE FUNNEL SIMULATION
# - funnel progression masks (controls stage drop-off)
# - synthetic hiring pipeline
# - interview stages
# - offer flow
# - enterprise pacing (speed + seniority)
# ============================================================


        # Funnel progression masks (controls stage drop-off)
        cv_review_mask = processed_chunk["Application Ref"].apply(
            lambda x: stable_hash("cv_" + x) % 100 < 65
        )

        sourcer_mask = cv_review_mask & (
            processed_chunk["Application Ref"].apply(
                lambda x: stable_hash("scr_" + x) % 100 < 45
            )
        )

        recruiter_mask = sourcer_mask & (
            processed_chunk["Application Ref"].apply(
                lambda x: stable_hash("rec_" + x) % 100 < 30
            )
        )

        hm_review_mask = recruiter_mask & (
            processed_chunk["Application Ref"].apply(
                lambda x: stable_hash("hmr_" + x) % 100 < 25
            )
        )

        hm_interview_mask = hm_review_mask & (
            processed_chunk["Application Ref"].apply(
                lambda x: stable_hash("hmi_" + x) % 100 < 18
            )
        )
        
        processed_chunk["Application/CV Review Date"] = pd.NaT

        processed_chunk.loc[cv_review_mask, "Application/CV Review Date"] = synthetic_step_date(
            base_date=processed_chunk.loc[cv_review_mask, "_applied_dt"],
            key=processed_chunk.loc[cv_review_mask, "Application Ref"],
            min_days=1,
            max_days=3,
            cap_date=processed_chunk.loc[cv_review_mask, "_end_dt"],
            seniority=processed_chunk.loc[cv_review_mask, "_seniority"]
        )

        processed_chunk["Sourcer Screen Date"] = pd.NaT

        processed_chunk.loc[sourcer_mask, "Sourcer Screen Date"] = synthetic_step_date(
            base_date=processed_chunk.loc[sourcer_mask, "Application/CV Review Date"],
            key=processed_chunk.loc[sourcer_mask, "Application Ref"],
            min_days=1,
            max_days=2,
            cap_date=processed_chunk.loc[sourcer_mask, "_end_dt"],
            seniority=processed_chunk.loc[sourcer_mask, "_seniority"]
        )

        processed_chunk["Recruiter Interview Date"] = pd.NaT

        processed_chunk.loc[recruiter_mask, "Recruiter Interview Date"] = synthetic_step_date(
            base_date=processed_chunk.loc[recruiter_mask, "Sourcer Screen Date"],
            key=processed_chunk.loc[recruiter_mask, "Application Ref"],
            min_days=1,
            max_days=4,
            cap_date=processed_chunk.loc[recruiter_mask, "_end_dt"],
            seniority=processed_chunk.loc[recruiter_mask, "_seniority"]
        )

        # Invalidate recruiter interview if it would occur before sourcer screen
        processed_chunk.loc[
            processed_chunk["Recruiter Interview Date"]
            < processed_chunk["Sourcer Screen Date"],
            "Recruiter Interview Date"
        ] = pd.NaT

        assessment_mask = (
            processed_chunk["Application Ref"]
            .apply(lambda x: stable_hash(x) % 100 < 40)
        )

        processed_chunk["Online Assessment/Test Date"] = pd.NaT

        processed_chunk.loc[assessment_mask, "Online Assessment/Test Date"] = synthetic_step_date(
            base_date=processed_chunk.loc[assessment_mask, "Recruiter Interview Date"],
            key=processed_chunk.loc[assessment_mask, "Application Ref"],
            min_days=1,
            max_days=4,
            cap_date=processed_chunk.loc[assessment_mask, "_end_dt"],
            seniority=processed_chunk["_seniority"]
        )

        processed_chunk["Recruiter Interview Date"] = pd.NaT

        processed_chunk.loc[recruiter_mask, "Recruiter Interview Date"] = synthetic_step_date(
            base_date=processed_chunk.loc[recruiter_mask, "Sourcer Screen Date"],
            key=processed_chunk.loc[recruiter_mask, "Application Ref"],
            min_days=1,
            max_days=4,
            cap_date=processed_chunk.loc[recruiter_mask, "_end_dt"],
            seniority=processed_chunk.loc[recruiter_mask, "_seniority"]
        )

        processed_chunk["CV submitted to Hiring Manager date"] = pd.NaT

        processed_chunk.loc[hm_review_mask, "CV submitted to Hiring Manager date"] = synthetic_step_date(
            base_date=processed_chunk.loc[hm_review_mask, "Recruiter Interview Date"],
            key=processed_chunk.loc[hm_review_mask, "Application Ref"],
            min_days=1,
            max_days=2,
            cap_date=processed_chunk.loc[hm_review_mask, "_end_dt"],
            seniority=processed_chunk.loc[hm_review_mask, "_seniority"]
        )
        
        processed_chunk["Hiring Manager Review Date"] = pd.NaT

        processed_chunk.loc[hm_review_mask, "Hiring Manager Review Date"] = synthetic_step_date(
            base_date=processed_chunk.loc[hm_review_mask, "CV submitted to Hiring Manager date"],
            key=processed_chunk.loc[hm_review_mask, "Application Ref"],
            min_days=2,
            max_days=5,
            cap_date=processed_chunk.loc[hm_review_mask, "_end_dt"],
            seniority=processed_chunk.loc[hm_review_mask, "_seniority"]
        )

        processed_chunk["Hiring Manager Review Completed Date"] = pd.NaT
        
        processed_chunk.loc[hm_review_mask, "Hiring Manager Review Completed Date"] = synthetic_step_date(
            base_date=processed_chunk.loc[hm_review_mask, "Hiring Manager Review Date"],
            key=processed_chunk.loc[hm_review_mask, "Application Ref"],
            min_days=1,
            max_days=2,
            cap_date=processed_chunk.loc[hm_review_mask, "_end_dt"],
            seniority=processed_chunk.loc[hm_review_mask, "_seniority"]
        )

        processed_chunk["Hiring Manager Interview Date"] = pd.NaT

        processed_chunk.loc[hm_interview_mask, "Hiring Manager Interview Date"] = synthetic_step_date(
            base_date=processed_chunk.loc[hm_interview_mask, "Hiring Manager Review Completed Date"],
            key=processed_chunk.loc[hm_interview_mask, "Application Ref"],
            min_days=3,
            max_days=7,
            cap_date=processed_chunk.loc[hm_interview_mask, "_end_dt"],
            seniority=processed_chunk.loc[hm_interview_mask, "_seniority"]
        )

        video_mask = (
            processed_chunk["Application Ref"]
            .apply(lambda x: stable_hash(x) % 100 < 50)
        )

        second_interview_mask = (
            processed_chunk["Application Ref"]
            .apply(lambda x: stable_hash(x) % 100 < 35)
        )

        assessment_mask = (
            processed_chunk["Application Ref"]
            .apply(lambda x: stable_hash(x) % 100 < 25)
        )

        processed_chunk["Video Interview Date"] = pd.NaT

        processed_chunk.loc[video_mask, "Video Interview Date"] = synthetic_step_date(
            base_date=processed_chunk.loc[video_mask, "Hiring Manager Interview Date"],
            key=processed_chunk.loc[video_mask, "Application Ref"],
            min_days=2,
            max_days=5,
            cap_date=processed_chunk.loc[video_mask, "_end_dt"],
            seniority=processed_chunk["_seniority"]
        )

        processed_chunk["2nd Interview Date"] = pd.NaT

        processed_chunk.loc[second_interview_mask, "2nd Interview Date"] = synthetic_step_date(
            base_date=processed_chunk.loc[second_interview_mask, "Video Interview Date"]
                .fillna(processed_chunk.loc[second_interview_mask, "Hiring Manager Interview Date"]),
            key=processed_chunk.loc[second_interview_mask, "Application Ref"],
            min_days=2,
            max_days=5,
            cap_date=processed_chunk.loc[second_interview_mask, "_end_dt"],
            seniority=processed_chunk["_seniority"]
        )

        processed_chunk["Assessment Centre Date"] = pd.NaT

        processed_chunk.loc[assessment_mask, "Assessment Centre Date"] = synthetic_step_date(
            base_date=processed_chunk.loc[assessment_mask, "2nd Interview Date"]
                .fillna(processed_chunk.loc[assessment_mask, "Video Interview Date"])
                .fillna(processed_chunk.loc[assessment_mask, "Hiring Manager Interview Date"]),
            key=processed_chunk.loc[assessment_mask, "Application Ref"],
            min_days=3,
            max_days=7,
            cap_date=processed_chunk.loc[assessment_mask, "_end_dt"],
            seniority=processed_chunk["_seniority"]
        )

        offer_base = (
            processed_chunk["Recruiter Interview Date"]
            .fillna(processed_chunk["Hiring Manager Interview Date"])
        )

        processed_chunk["Offer Made Date"] = pd.NaT

        offer_mask = hm_interview_mask & (
            processed_chunk["Application Ref"].apply(
                lambda x: stable_hash("offer_" + x) % 100 < 15
            )
        )

        processed_chunk.loc[offer_mask, "Offer Made Date"] = synthetic_step_date(
            base_date=offer_base.loc[offer_mask],
            key=processed_chunk.loc[offer_mask, "Application Ref"],
            min_days=5,
            max_days=25,
            cap_date=processed_chunk.loc[offer_mask, "_end_dt"],
            seniority=processed_chunk["_seniority"]
        )

        offer_min = processed_chunk["Offer Made Date"].dropna().min()

        if pd.notna(offer_min):
            assert (
                offer_min
                < processed_chunk["_applied_dt"].max() - pd.Timedelta(days=365)
            ), "Offer funnel not sufficiently distributed"

        accept_mask = offer_mask & (
            processed_chunk["Application Ref"].apply(
                lambda x: stable_hash("accept_" + x) % 100 < 70
            )
        )

        processed_chunk["Verbal Offer Made Date"] = pd.NaT

        processed_chunk.loc[offer_mask, "Verbal Offer Made Date"] = synthetic_step_date(
            base_date=processed_chunk.loc[offer_mask, "Offer Made Date"],
            key=processed_chunk.loc[offer_mask, "Application Ref"],
            min_days=0,
            max_days=2,
            cap_date=processed_chunk.loc[offer_mask, "_end_dt"],
            seniority=processed_chunk["_seniority"]
        )

        processed_chunk["Verbal Offer Accepted Date"] = pd.NaT

        processed_chunk.loc[accept_mask, "Verbal Offer Accepted Date"] = synthetic_step_date(
            base_date=processed_chunk.loc[accept_mask, "Verbal Offer Made Date"],
            key=processed_chunk.loc[accept_mask, "Application Ref"],
            min_days=0,
            max_days=1,
            cap_date=processed_chunk.loc[accept_mask, "_end_dt"],
            seniority=processed_chunk["_seniority"]
        )

        processed_chunk["Written Offer Made Date"] = pd.NaT

        processed_chunk.loc[accept_mask, "Written Offer Made Date"] = synthetic_step_date(
            base_date=processed_chunk.loc[accept_mask, "Verbal Offer Accepted Date"],
            key=processed_chunk.loc[accept_mask, "Application Ref"],
            min_days=0,
            max_days=1,
            cap_date=processed_chunk.loc[accept_mask, "_end_dt"],
            seniority=processed_chunk["_seniority"]
        )

        processed_chunk["Written Offer Accepted Date"] = pd.NaT

        processed_chunk.loc[accept_mask, "Written Offer Accepted Date"] = synthetic_step_date(
            base_date=processed_chunk.loc[accept_mask, "Written Offer Made Date"],
            key=processed_chunk.loc[accept_mask, "Application Ref"],
            min_days=0,
            max_days=2,
            cap_date=processed_chunk.loc[accept_mask, "_end_dt"],
            seniority=processed_chunk["_seniority"]
        )

        processed_chunk["Offer Accepted Date"] = pd.NaT

        processed_chunk.loc[
            accept_mask,
            "Offer Accepted Date"
        ] = processed_chunk.loc[
            accept_mask,
            "Written Offer Accepted Date"
        ]

        processed_chunk.loc[
            processed_chunk["Offer Accepted Date"].isna(),
            "Start Date"
        ] = pd.NaT

        processed_chunk["Security/Background Check Date"] = pd.NaT

        processed_chunk.loc[processed_chunk["Hired"], "Security/Background Check Date"] = synthetic_step_date(
            base_date=processed_chunk.loc[processed_chunk["Hired"], "Offer Accepted Date"],
            key=processed_chunk.loc[processed_chunk["Hired"], "Application Ref"],
            min_days=2,
            max_days=10,
            cap_date=processed_chunk.loc[processed_chunk["Hired"], "_end_dt"],
            seniority=processed_chunk["_seniority"]
        )

        processed_chunk["Ready to Hire Date"] = pd.NaT

        processed_chunk.loc[processed_chunk["Hired"], "Ready to Hire Date"] = synthetic_step_date(
            base_date=processed_chunk.loc[processed_chunk["Hired"], "Security/Background Check Date"],
            key=processed_chunk.loc[processed_chunk["Hired"], "Application Ref"],
            min_days=0,
            max_days=1,
            cap_date=processed_chunk.loc[processed_chunk["Hired"], "_end_dt"],
            seniority=processed_chunk["_seniority"]
        )

        def dropout_delay(app_ref):
            """
            Enterprise pipelines often reject later, not instantly.
            """
            v = stable_hash(app_ref) % 100

            if v < 30:
                return 2   # fast rejection
            elif v < 70:
                return 7   # typical delay
            else:
                return 14  # long pipeline stall

        processed_chunk["Rejection Date"] = pd.NaT

        reject_mask = processed_chunk["Candidate status"].str.contains("reject", na=False)

        reject_subset = processed_chunk.loc[reject_mask]

        processed_chunk.loc[reject_mask, "Rejection Date"] = (
            reject_subset["Recruiter Interview Date"]
            + pd.to_timedelta(
                 reject_subset["Application Ref"].apply(dropout_delay),
                unit="D"
            )
        )

        processed_chunk["Withdrawal Date"] = pd.NaT

        withdraw_mask = processed_chunk["Candidate status"].str.contains("declined by candidate", na=False)

        withdraw_subset = processed_chunk.loc[withdraw_mask]

        processed_chunk.loc[withdraw_mask, "Withdrawal Date"] = (
            withdraw_subset["_applied_dt"]
            + pd.to_timedelta(
                synthetic_offset_days(
                    withdraw_subset["Application Ref"],
                    min_days=1,
                    max_days=5
                ),
                unit="D"
            )
        )

        processed_chunk.loc[
            processed_chunk["Rejection Date"].notna(),
            "_end_dt"
        ] = processed_chunk.loc[
            processed_chunk["Rejection Date"].notna(),
            "Rejection Date"
        ]

        processed_chunk.loc[
            processed_chunk["Withdrawal Date"].notna(),
            "_end_dt"
        ] = processed_chunk.loc[
            processed_chunk["Withdrawal Date"].notna(),
            "Withdrawal Date"
        ]

        processed_chunk["Application Initiated Date"] = processed_chunk["_applied_dt"]

        processed_chunk["Application Reason For Withdrawal"] = None

        processed_chunk.loc[withdraw_mask, "Application Reason For Withdrawal"] = (
            processed_chunk.loc[withdraw_mask, "Application Ref"]
            .apply(lambda x: "Candidate withdrew")
        )

        processed_chunk["Application Reason For Rejection"] = None

        processed_chunk.loc[reject_mask, "Application Reason For Rejection"] = (
            processed_chunk.loc[reject_mask, "Application Ref"]
            .apply(lambda x: "Not a fit for role")
        )

        # Withdrawn candidates never get interviewed or offered
        processed_chunk.loc[withdraw_mask, ["Recruiter Interview Date", "Offer Accepted Date"]] = pd.NaT

        # Rejected candidates never get offers
        processed_chunk.loc[reject_mask, "Offer Accepted Date"] = pd.NaT

        processed_chunk["Start Date"] = pd.to_datetime(
            processed_chunk["Start Date"],
            dayfirst=True,
            errors="coerce"
        )

        processed_chunk["Offer Accepted Date"] = pd.to_datetime(
            processed_chunk["Offer Accepted Date"],
            errors="coerce"
        )

        mask = processed_chunk["Recruiter Interview Date"].notna()

        assert (
            processed_chunk.loc[mask, "Application Initiated Date"]
            <= processed_chunk.loc[mask, "Recruiter Interview Date"]
        ).all(), "Initiated Date occurs after Recruiter Interview"

        mask = (
            processed_chunk["Offer Accepted Date"].notna()
            & processed_chunk["_end_dt"].notna()
        )

        assert (
            processed_chunk.loc[mask, "Offer Accepted Date"]
            <= processed_chunk.loc[mask, "_end_dt"]
        ).all(), "Offer Accepted after pipeline end detected"
        
        # Candidate Reneged logic
        processed_chunk["Candidate Reneged"] = "No"
        processed_chunk["Candidate Reneged Date"] = pd.NaT

        reneged_eligible_mask = (
            (processed_chunk["Hired"] == True)
            & processed_chunk["Offer Accepted Date"].notna()
            & processed_chunk["Start Date"].notna()
            & (
                processed_chunk["Start Date"]
                > processed_chunk["Offer Accepted Date"] + pd.Timedelta(days=1)
            )
        )

        reneged_mask = (
            reneged_eligible_mask
            & (processed_chunk["Application Ref"].apply(lambda x: stable_hash(x) % 100 < 8))
        )

        processed_chunk.loc[reneged_mask, "Candidate Reneged"] = "Yes"

        # Step 1: generate reneged date after offer acceptance
        valid_mask = (
            reneged_mask
            & processed_chunk["Offer Accepted Date"].notna()
            & processed_chunk["Start Date"].notna()
        )

        processed_chunk.loc[valid_mask, "Candidate Reneged Date"] = synthetic_step_date(
            base_date=processed_chunk.loc[valid_mask, "Offer Accepted Date"],
            key=processed_chunk.loc[valid_mask, "Application Ref"],
            min_days=3,
            max_days=14,
            cap_date=processed_chunk.loc[valid_mask, "Start Date"] - pd.Timedelta(days=1),
            seniority=processed_chunk.loc[valid_mask, "_seniority"]
        )

        # Step 2: ensure reneged date is before start date
        processed_chunk.loc[
            reneged_mask
            & (
                processed_chunk["Candidate Reneged Date"]
                >= processed_chunk["Start Date"]
            ),
            "Candidate Reneged Date"
        ] = (
            processed_chunk.loc[
                reneged_mask
                & (
                    processed_chunk["Candidate Reneged Date"]
                    >= processed_chunk["Start Date"]
                ),
                "Start Date"
            ]
            - pd.Timedelta(days=1)
        )

        processed_chunk.loc[reneged_mask, "Start Date"] = pd.NaT
        processed_chunk.loc[reneged_mask, "Ready to Hire Date"] = pd.NaT
        processed_chunk.loc[reneged_mask, "Security/Background Check Date"] = pd.NaT

        processed_chunk.loc[reneged_mask, "Candidate status"] = "reneged"


# ============================================================
# STAGE 3 — CANDIDATE PROFILE SYNTHESIS
# - demographics
# - education
# - age modelling
# - graduation logic
# ============================================================


        processed_chunk = processed_chunk.drop(
            columns=[c for c in ["Source", "Source Category"] if c in processed_chunk.columns],
            errors="ignore"
        )

        assigned_sources = processed_chunk["Application Ref"].apply(
            lambda x: assign_source_from_reference(x, source_reference)
        )

        processed_chunk["Source"] = assigned_sources.apply(lambda x: x[0])
        processed_chunk["Source Medium"] = assigned_sources.apply(lambda x: x[1])

        processed_chunk["Religion"] = processed_chunk["Candidate Ref"].apply(
            lambda x: assign_from_enum(x, RELIGION_VALUES)
        )

        processed_chunk["Indicated Disability Status"] = processed_chunk["Candidate Ref"].apply(
            lambda x: assign_from_enum(x, DISABILITY_VALUES)
        )

        processed_chunk["Degree Name"] = processed_chunk["Candidate Ref"].apply(
            lambda x: assign_from_enum(x, DEGREE_VALUES)
        )

        if i == 0:
            # Applied Date must reach MAX_DATE
            assert processed_chunk["_applied_dt"].max() <= MAX_DATE


        # === Candidate Profile Enrichment ===
        AGE_BANDS = {
            "junior": (22, 28),
            "mid": (26, 38),
            "senior": (32, 50),
            "exec": (38, 60),
        }

        def assign_age(candidate_ref, band):
            min_age, max_age = AGE_BANDS[band]
            return min_age + (stable_hash(candidate_ref) % (max_age - min_age + 1))

        # NOTE: row-wise apply kept for readability.
        # In production-scale pipelines this would be vectorised or NumPy-backed.
        processed_chunk["_age"] = processed_chunk.apply(
            lambda r: assign_age(r["Candidate Ref"], r["_seniority"]),
            axis=1
        )

        processed_chunk["Age / DOB"] = (
            processed_chunk["_applied_dt"]
            - pd.to_timedelta(processed_chunk["_age"] * 365, unit="D")
        )

        EDUCATION_BY_SENIORITY = {
            "junior": ["Secondary education", "Post-secondary school", "Tertiary education"],
            "mid": ["Tertiary education"],
            "senior": ["Tertiary education", "Post-graduate education"],
            "exec": ["Post-graduate education"],
        }

        def assign_education(candidate_ref, band):
            options = EDUCATION_BY_SENIORITY[band]
            return options[stable_hash(candidate_ref) % len(options)]

        # NOTE: row-wise apply kept for readability.
        # In production-scale pipelines this would be vectorised or NumPy-backed.
        processed_chunk["Education Level"] = processed_chunk.apply(
            lambda r: assign_education(r["Candidate Ref"], r["_seniority"]),
            axis=1
        )

        NO_DEGREE_ALLOWED_LEVELS = [
            "Basic education",
            "Secondary education",
            "Post-secondary school",
        ]

        mask = processed_chunk["Degree Name"] == "No Degree"

        processed_chunk.loc[mask, "Education Level"] = (
            processed_chunk.loc[mask, "Candidate Ref"]
            .apply(lambda x: NO_DEGREE_ALLOWED_LEVELS[stable_hash(x) % len(NO_DEGREE_ALLOWED_LEVELS)])
        )

        def assign_graduated(candidate_ref, band):
            threshold = 70 if band == "junior" else 95
            return "Yes" if (stable_hash(candidate_ref) % 100) < threshold else "No"

        processed_chunk["Graduated"] = processed_chunk.apply(
            lambda r: assign_graduated(r["Candidate Ref"], r["_seniority"]),
            axis=1
        )

        processed_chunk["Graduation Date"] = pd.NaT

        grad_mask = processed_chunk["Graduated"] == "Yes"

        processed_chunk.loc[grad_mask, "Graduation Date"] = (
            processed_chunk.loc[grad_mask, "_applied_dt"]
            - pd.to_timedelta(
                processed_chunk.loc[grad_mask, "Candidate Ref"]
                .apply(lambda x: 365 * (1 + (stable_hash(x) % 10))),
                unit="D"
            )
        )

        def assign_notice_period(seniority):
            if seniority == "junior":
                return "2 weeks"
            if seniority == "mid":
                return "1 month"
            if seniority == "senior":
                return "2 months"
            return "3 months"

        processed_chunk["Notice Period"] = processed_chunk["_seniority"].apply(assign_notice_period)

        INSTITUTIONS_BY_EDU = {
            "Basic education": [
                "Local Primary School",
                "Community School",
            ],
            "Secondary education": [
                "General Secondary School",
                "Technical Secondary School",
            ],
            "Post-secondary school": [
                "Vocational Training Institute",
                "Professional College",
            ],
            "Tertiary education": [
                "National University",
                "Technical University",
                "University of Applied Sciences",
            ],
            "Post-graduate education": [
                "Business School",
                "Graduate School of Management",
                "Institute of Advanced Studies",
            ],
        }

        def assign_institution(candidate_ref, edu_level):
            options = INSTITUTIONS_BY_EDU.get(
                edu_level,
                ["Independent Education Institute"]
            )
            return options[stable_hash(candidate_ref) % len(options)]

        # NOTE: row-wise apply kept for readability.
        # In production-scale pipelines this would be vectorised or NumPy-backed.
        processed_chunk["Institution"] = processed_chunk.apply(
            lambda r: assign_institution(r["Candidate Ref"], r["Education Level"]),
            axis=1
        )

        COUNTRY_TO_NATIONALITY = {
            "Poland": ["Polish"],
            "Germany": ["German"],
            "France": ["French"],
            "United Kingdom": ["British"],
            "Spain": ["Spanish"],
            "Italy": ["Italian"],
            "Netherlands": ["Dutch"],
            "United States": ["American"],
            "Canada": ["Canadian"],
        }

        def assign_nationality(candidate_ref, country):
            if country in COUNTRY_TO_NATIONALITY:
                options = COUNTRY_TO_NATIONALITY[country]
            else:
                options = ["Other"]

            return options[stable_hash(candidate_ref) % len(options)]

        processed_chunk["Nationality"] = processed_chunk.apply(
            lambda r: assign_nationality(r["Candidate Ref"], r["Address Country"]),
            axis=1
        )

        MILITARY_ELIGIBLE_NATIONALITIES = {
            "Polish",
            "German",
            "French",
            "American",
        }

        def assign_military_service(candidate_ref, dob, nationality):
            age = (MAX_DATE - dob).days // 365

            if age < 20 or age > 45:
                return "No"

            if nationality not in MILITARY_ELIGIBLE_NATIONALITIES:
                return "No"

            # ~15% Yes
            return "Yes" if (stable_hash(candidate_ref) % 100) < 15 else "No"

        # NOTE: row-wise apply kept for readability.
        # In production-scale pipelines this would be vectorised or NumPy-backed.
        processed_chunk["Military Service"] = processed_chunk.apply(
            lambda r: assign_military_service(
                r["Candidate Ref"],
                r["Age / DOB"],
                r["Nationality"],
            ),
            axis=1
        )

        if i == 0:

            assert processed_chunk["Institution"].notna().all()
            assert processed_chunk["Nationality"].notna().all()
            assert processed_chunk["Military Service"].isin(["Yes", "No"]).all()

 
# ============================================================
# STAGE 4 — EMPLOYMENT ATTRIBUTES
# - contract modeling
# - working hours
# - adjustments
# - hired country
# ============================================================


        CONTRACT_TYPES = ["Permanent", "FTC", "Contract", "Consultancy"]

        def assign_contract_type(app_ref):
            return CONTRACT_TYPES[stable_hash(app_ref) % len(CONTRACT_TYPES)]

        processed_chunk["Contract Type"] = processed_chunk["Application Ref"].apply(assign_contract_type)

        def assign_hours(contract_type, app_ref):
            if contract_type == "Permanent":
                return 40
            if contract_type == "FTC":
                return 40
            if contract_type == "Contract":
                return 40 if (stable_hash(app_ref) % 2 == 0) else 20
            return 20  # Consultancy

        # NOTE: row-wise apply kept for readability.
        # In production-scale pipelines this would be vectorised or NumPy-backed.
        processed_chunk["Hours per Week"] = processed_chunk.apply(
            lambda r: assign_hours(r["Contract Type"], r["Application Ref"]),
            axis=1
        )

        def assign_adjustments(candidate_ref):
            return "Yes" if (stable_hash(candidate_ref) % 100) < 8 else "No"

        processed_chunk["Reasonable Adjustments"] = processed_chunk["Candidate Ref"].apply(assign_adjustments)

        processed_chunk["Hired Country"] = None

        hired_mask = processed_chunk["Hired"] == True

        # Primary: Address Country
        processed_chunk.loc[hired_mask, "Hired Country"] = (
            processed_chunk.loc[hired_mask, "Address Country"]
        )

        # Fallback: use Requisition-based country if Address Country is missing
        processed_chunk.loc[
            hired_mask & processed_chunk["Hired Country"].isna(),
            "Hired Country"
        ] = "Unknown"

        if i == 0:

            assert processed_chunk["Contract Type"].notna().all()
            assert processed_chunk["Hours per Week"].notna().all()
            assert processed_chunk["Notice Period"].notna().all()
            assert processed_chunk["Reasonable Adjustments"].isin(["Yes", "No"]).all()
            assert processed_chunk.loc[processed_chunk["Hired"] == True, "Hired Country"].notna().all()


# ============================================================
# STAGE 5 — VALIDATION LAYER
# - enterprise data integrity checks
# - funnel ordering validation
# - reneged safeguards
# ============================================================


        # === DATE ORDER VALIDATION (debug / first chunk only) ===
        if i == 0:

            mask = (
                processed_chunk["Application/CV Review Date"].notna()
                & processed_chunk["Sourcer Screen Date"].notna()
            )

            assert (
                processed_chunk.loc[mask, "Application/CV Review Date"]
                <= processed_chunk.loc[mask, "Sourcer Screen Date"]
            ).all(), "CV Review after Sourcer Screen detected"

            mask = (
                processed_chunk["Sourcer Screen Date"].notna()
                & processed_chunk["Recruiter Interview Date"].notna()
            )           
            
            assert (
                    processed_chunk.loc[mask, "Sourcer Screen Date"]
                    <= processed_chunk.loc[mask, "Recruiter Interview Date"]
            ).all(), "Sourcer Screen after Recruiter Interview detected"

            mask = (
                processed_chunk["Offer Made Date"].notna() 
                & processed_chunk["Written Offer Accepted Date"].notna()
            )

            assert (
                processed_chunk.loc[mask, "Offer Made Date"]
                <= processed_chunk.loc[mask, "Written Offer Accepted Date"]
            ).all()

            assert (
                processed_chunk["Ready to Hire Date"].dropna()
                <= MAX_DATE
            ).all(), "Ready to Hire exceeds MAX_DATE"

            assert pd.api.types.is_datetime64_any_dtype(processed_chunk["Start Date"])
            assert pd.api.types.is_datetime64_any_dtype(processed_chunk["Offer Accepted Date"])

            assert (
                processed_chunk.loc[processed_chunk["Candidate Reneged"] == "Yes", "Hired"]
                == True
            ).all(), "Reneged candidate not hired"

            assert (
                processed_chunk.loc[processed_chunk["Candidate Reneged"] == "Yes", "Candidate Reneged Date"]
                > processed_chunk.loc[processed_chunk["Candidate Reneged"] == "Yes", "Offer Accepted Date"]
            ).all(), "Reneged before offer acceptance"

            assert (
                processed_chunk.loc[processed_chunk["Candidate Reneged"] == "Yes", "Start Date"]
                .isna()
            ).all(), "Reneged candidate has Start Date"


        # === EDUCATION BLOCK VALIDATION ===
        
        if i == 0:

            assert (
                processed_chunk.loc[processed_chunk["Graduated"] == "Yes", "Graduation Date"]
                < processed_chunk.loc[processed_chunk["Graduated"] == "Yes", "_applied_dt"]
            ).all(), "Graduation after application detected"

            assert (
                processed_chunk.loc[processed_chunk["Graduated"] == "No", "Graduation Date"]
                .isna()
            ).all(), "Graduation date exists for non-graduate"

            assert (
                processed_chunk.loc[processed_chunk["Degree Name"] == "No Degree", "Education Level"]
                .isin(NO_DEGREE_ALLOWED_LEVELS)
            ).all(), "Invalid Education Level for No Degree"
            

            assert (
                processed_chunk.loc[processed_chunk["Graduated"] == "Yes", "Graduation Date"]
                <= processed_chunk.loc[processed_chunk["Graduated"] == "Yes", "_applied_dt"]
            ).all()


# ============================================================
# STAGE 6 — OUTPUT FINALIZATION
# - final column sync
# - helper cleanup
# - export
# ============================================================

        
        processed_chunk["Source Name"] = processed_chunk["Source"]
        processed_chunk["City"] = processed_chunk["Address City"]

        # --- Sync Applied Date with enterprise timeline ---
        processed_chunk["Applied Date"] = processed_chunk["_applied_dt"]

        
        # === SENIORITY AND AGE HELPERS CLEAN UP ===
        processed_chunk.drop(
            columns=["_seniority", "_age"],
            inplace=True,
            errors="ignore"
        )
        

        # === CLEAN UP HELPERS ===
        processed_chunk.drop(
            columns=["_applied_dt", "_end_dt"],
            inplace=True,
            errors="ignore"
        )
        
        # === ADD CLIENT INFO ===
        processed_chunk["Company Name"] = client_name
        processed_chunk["Client Code"] = client_code


        # === FINAL OUTPUT ===
        processed_chunk = processed_chunk[FINAL_OUTPUT_COLUMNS]

        remaining = target_applications - processed_rows

        if len(processed_chunk) > remaining:
            processed_chunk = processed_chunk.head(remaining)

        processed_rows += len(processed_chunk)

        if i == 0:
            print("Earliest Offer Made:", processed_chunk["Offer Made Date"].min())

        if i == 0:
            print("DEBUG – final columns being written:")
            print(list(processed_chunk.columns))

        if i == 0:
            assert processed_chunk["Requisition ID"].notna().all()
            assert processed_chunk["Job Title"].notna().all()

        if i == 0:
            assert processed_chunk["Source"].isin(source_reference["Source"]).all()
            assert processed_chunk["Source Medium"].isin(source_reference["Source Medium"]).all()
        
        processed_chunk.to_csv(
            OUTPUT_PATH,
            mode="a",
            header=(i == 0),
            index=False
        )

    print("Finished processing all chunks.")

if __name__ == "__main__":  
    
    for client in CLIENTS:
        process(
            client_code=client["code"],
            client_name=client["name"],
            share=client["share"]
        )