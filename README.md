# Requisitions Data Anonymization Pipeline

## Project Overview

This project implements a production-ready anonymization framework for recruitment requisition data.

The goal is to transform raw, client-specific recruitment data into a fully anonymized, realistic, and analytically usable dataset that can be safely shared for:
- BI development
- Data model design
- Analytics use cases
- Portfolio and demo purposes

The solution is designed to be client-agnostic, reusable across multiple organizations, and aligned with real-world recruitment processes.

## Business Problem & Use Case

Recruitment data often contains highly sensitive information, including personal data, internal structures, compensation ranges, and operational timelines.  
Sharing such data for analytics development, dashboard prototyping, or stakeholder demonstrations requires a robust anonymization approach that maintains realism without exposing confidential information.

The goal of this project is to:
- Enable analytics and data modeling work without access to real client data
- Preserve meaningful recruitment logic (e.g. workflows, timelines, job structures)
- Support centralized reporting models across multiple clients

### Solution

This project solves the problem by providing an automated anonymization pipeline that:
- removes or obfuscates all sensitive identifiers
- preserves realistic distributions, timelines, and relationships
- keeps the dataset fully usable for reporting, analytics, and BI modeling

The output can be safely shared while still behaving like real production data.

## Data Sources & Inputs

The project operates on structured HR data exported from an Applicant Tracking System (ATS).

### Input Files

| File name | Description |
|---------|-------------|
| `requisitions_raw.xlsx` | Raw ATS requisition export containing sensitive and identifiable data |
| `job_reference_table.xlsx` | Curated job taxonomy reference used to generate realistic job attributes |
| `geo_reference.csv` | Country-level geographic reference for deriving city, region, and currency |

### Key Characteristics of Input Data
- non-standard Excel header layout
- mixed data types (dates, text, numeric)
- free-text fields containing sensitive content
- inconsistent population of optional fields

The script is designed to handle these challenges defensively and consistently.

## Anonymization Strategy

The anonymization logic focuses on structural realism rather than raw data masking.

Key principles:
- No client-specific identifiers, names, or keywords are hard-coded
- All anonymization logic is deterministic or controlled-random for consistency
- Original business logic and relationships are preserved

### Applied Techniques

- **Identifiers**
  - Job Requisition IDs are replaced with sequential surrogate keys

- **People**
  - All personal names (Hiring Managers, Recruiters, Sourcers) are replaced using Faker
  - No original names or emails are retained

- **Organizational Structure**
  - Operating Structure values are anonymized using generated dimension codes

- **Job Taxonomy**
  - Job Posting Titles are replaced with values from a reference dimension
  - Job Family, Job Grade, and Programme Type are derived consistently from the reference table

- **Compensation**
  - Salary ranges are generated based on Job Grade bands
  - Values are realistic but non-identifiable

- **Geography**
  - City, County, and Currency are derived from a country-level reference table

- **Dates**
  - Original date values from the raw file are preserved where appropriate to maintain realistic timelines
  - Derived dates (e.g. Approved Date, Posting Dates) are generated using logical offsets

- **Free Text**
  - All free-text fields are explicitly cleared

## Date Handling & Business Logic

Special care is taken to ensure temporal realism:

- Original dates from the raw dataset are preserved where appropriate
- Synthetic dates are generated only when missing or required
- All dates follow logical recruitment sequences:
  - Intake Meeting → Approved Date → Posting Dates → Target Hire Date → Contract End Date

### Date Validation Rules
- No date exceeds **31 January 2026**
- All generated dates are capped and validated
- The script includes a final assertion to guarantee temporal integrity

This approach avoids unrealistic scenarios such as approvals or hiring dates occurring in the future.

## Technical Architecture & Processing Flow

The anonymization process is implemented as a single, reproducible Python pipeline designed for clarity, auditability, and extensibility.

### High-Level Flow
1. Load reference dimensions (job taxonomy, geography)
2. Load raw requisitions extract
3. Create anonymized working copy
4. Apply anonymization and derivation steps in logical order
5. Select and rename final output schema
6. Export anonymized dataset

### Processing Order
The script is intentionally ordered to avoid data dependency issues:

1. **Reference data loading**
   - Job reference table
   - Geography reference table

2. **Primary identifiers**
   - Job Requisition ID anonymization is applied first
   - Ensures consistent joins and downstream references

3. **People anonymization**
   - Names are normalized before mapping
   - Deterministic mapping ensures consistency across columns

4. **Organizational anonymization**
   - Operating Structure anonymized after people
   - Prevents accidental re-identification via hierarchy

5. **Job taxonomy transformation**
   - Job Posting Titles are replaced using controlled vocabulary
   - Job Family, Grade, and Programme Type derived from reference

6. **Geographic enrichment**
   - City, County, and Currency derived from country reference
   - Ensures realism without leaking location data

7. **Date transformation**
   - Global date offset applied
   - Preserves time intervals and funnel logic

8. **Business logic derivations**
   - Workflow Name
   - Posting logic (internal/external)
   - Pipeline and evergreen indicators

9. **Final schema enforcement**
   - Only required columns are retained
   - Columns renamed to target reporting schema

### Technologies Used
- Python 3.14
- pandas
- numpy
- Faker

## Data Quality & Validation

Several safeguards are built into the pipeline to ensure data quality and analytical usefulness after anonymization.

### Validation Checks
- Column names are normalized immediately after loading source files
- Missing job reference mappings are detected and logged
- Geographic lookups fall back to `"Unknown"` values when no match is found
- Blanks in boolean-style fields are explicitly handled (e.g. Evergreen roles default to "No")

### Consistency Guarantees
- The same original value always maps to the same anonymized value within a run
- Date offsets preserve relative timing between events
- Salary ranges remain logically consistent with job grades
- Job taxonomy relationships (Title → Family → Grade → Programme) are preserved

### Defensive Defaults
- Unexpected or missing values never break execution
- Text fields that may contain sensitive data are fully cleared
- All transformations remain unchanged within a single run

## Privacy & Compliance Considerations

- The script contains no references to real company names, brands, or identifiers
- All anonymization logic is reusable across clients
- Free-text fields are fully removed to eliminate leakage risk
- The output dataset is suitable for:
  - Internal demos
  - Analytics development
  - Training and portfolio presentation

This approach aligns with common GDPR and data-minimization principles.

## Key Design Principles

The anonymization logic follows several core principles:

- **Realism over randomization**  
  Synthetic data preserves real-world recruitment logic (dates, workflows, dependencies).

- **Reference-driven consistency**  
  Job attributes, grades, programmes, and salary ranges are derived from reference tables.

- **Temporal integrity**  
  All generated dates follow a logical sequence and are capped at a fixed maximum date.

- **Client safety by design**  
  No client-identifying strings, values, or business logic are hardcoded.

- **Reusability**  
  The script can be reused across multiple clients with minimal configuration changes.

## Known Limitations

This project intentionally focuses on anonymization quality and analytical realism rather than full production hardening.

### Current Limitations
- Reference data must be kept up to date manually
- The pipeline assumes consistent column naming in the source extract
- No automated unit tests are included
- Geographic enrichment uses random sampling within country, not weighted distributions

### Non-Goals
- This project does not aim to preserve exact headcount distribution
- It does not attempt to mask outliers using advanced statistical techniques
- It is not designed for real-time or streaming data

## Possible Extensions

This pipeline is designed to be easily extendable.

### Technical Extensions
- Add automated unit tests (pytest)
- Introduce schema validation using pandera or Great Expectations
- Externalize configuration into YAML or JSON
- Add logging instead of print statements
- Package as a reusable Python module

### Data & Analytics Extensions
- Support multiple anonymization profiles (light / strict)
- Introduce synthetic time-series generation
- Weight geographic sampling by hiring volume
- Add candidate-level anonymization for ATS datasets
- Generate data quality metrics post-anonymization

### Platform Extensions
- Containerize with Docker
- Schedule via Airflow or Azure Data Factory
- Store outputs in cloud storage (Azure Blob / S3)

## How to Run

1. Clone the repository
2. Place required input files in the `requisitions/` folder
3. Navigate to the `scripts/` directory
4. Run:

```bash
python anonymize_requisitions.py

The anonymized output file will be generated in the project directory.

### Required Files
requisitions_raw.xlsx - raw ATS extract (not included)

job_reference_table.xlsx - job taxonomy reference

geo_reference.csv - geographic reference data

anonymize_requisitions.py - main script

## Portfolio & Interview Use

This project demonstrates:

- Data anonymization design
- Recruitment domain knowledge
- Dimensional modeling awareness
- Business-rule-driven data engineering
- Defensive programming and validation

It is intended as both a portfolio project and a foundation for building a centralized recruitment data model.
