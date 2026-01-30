# Requisitions Data Anonymization Pipeline

## Project Overview

This project demonstrates a production-style Python pipeline for anonymizing ATS requisition data while preserving analytical value for BI and reporting use cases.

The script transforms a raw requisitions export into a fully anonymized, analytics-ready dataset by:
- removing or anonymizing personal and sensitive information
- preserving realistic business logic and data relationships
- maintaining analytical usability for BI, reporting, and demos

The solution is designed to be:
- compliant with data privacy requirements
- reproducible and deterministic
- suitable for sharing with stakeholders, interviewers, and portfolio reviews

## Business Problem & Use Case

HR and Talent Acquisition teams frequently need to share requisition-level data for:
- BI dashboards and analytics development
- system demos and proof-of-concept work
- training and internal enablement
- external vendors or consulting engagements

However, raw ATS exports contain:
- personally identifiable information (PII)
- sensitive organizational structures
- confidential salary and hiring details

This creates a conflict between:
- data privacy and compliance requirements
- the need for realistic, analyzable datasets

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

The goal of anonymization is to remove or obscure all sensitive and personally identifiable information (PII) while preserving analytical usefulness and internal consistency.

### Identifiers
- **Job Requisition ID** values are replaced with synthetic IDs (`REQ_000001`, `REQ_000002`, …)
- Referential integrity is preserved across the dataset

### People & Personal Data
- Names of hiring managers, recruiters, and sourcers are replaced with realistic synthetic names generated using Faker
- The same real person is always mapped to the same anonymized identity within a single run
- No emails, usernames, or free-text identifiers are preserved

### Organizational Data
- Organizational structures (e.g. Operating Structure) are anonymized using deterministic labels
- Original hierarchy depth is preserved while removing identifiable naming

### Job & HR Taxonomy
- Job Posting Titles are replaced using a controlled vocabulary from a reference table
- Related attributes are derived logically:
  - Job Family
  - Job Grade
  - Programme Type
- This ensures internally consistent HR data suitable for analytics and demos

### Location & Geography
- City, region, and currency are derived from a country-level reference table
- Values are randomly sampled per record but remain realistic and geographically valid

### Compensation
- Salary ranges are generated based on job grade
- Values are realistic but not traceable to any real compensation data

### Dates
- All dates are shifted by a single random offset
- Relative timing between events is preserved
- Seasonal and pipeline patterns remain intact

### Free-Text Fields
- Free-text fields are fully cleared to eliminate any risk of hidden sensitive data
- A defensive global scrub removes company-specific references

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

### Design Principles
- **Deterministic where required** (IDs, people mapping)
- **Randomized where safe** (cities, salaries, dates)
- **Stateless execution** (no external dependencies)
- **Readable and maintainable** (single-pass pipeline)

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

## How to Run the Project

### Prerequisites
- Python 3.9+
- pandas
- numpy
- Faker

Install dependencies:

pip install pandas numpy faker

### Required Files
requisitions_raw.xlsx - raw ATS extract (not included)

job_reference_table.xlsx - job taxonomy reference

geo_reference.csv - geographic reference data

anonymize_requisitions.py - main script

### Run the Script
python anonymize_requisitions.py

### Output
requisitions_anonymized.xlsx

Fully anonymized, analytics-ready dataset

Safe for demos, testing, and BI development
