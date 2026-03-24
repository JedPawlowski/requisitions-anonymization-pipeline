# 🚀 Recruitment Data Anonymization & Analytics Platform

**Python + Power BI | End-to-End Data Engineering & BI Solution**

---

## 📌 Project Overview

This project demonstrates an **end-to-end recruitment analytics platform** that transforms raw Applicant Tracking System (ATS) data into a **fully anonymized, analytics-ready dataset** and delivers a **production-grade Power BI data model with KPI layer**.

It simulates a real-world enterprise environment where sensitive recruitment data must be protected while still enabling analytics, reporting, and data modeling.

---

## 🎯 Business Problem

Recruitment data contains highly sensitive information:

* personal data (candidates, recruiters)
* organizational structures
* compensation data
* hiring timelines

This creates challenges for:

* building dashboards
* sharing data across teams
* developing analytics solutions safely

---

## 💡 Solution

This project combines:

### 🔹 Python Data Pipeline

* anonymizes all sensitive data
* generates realistic synthetic data
* preserves business logic and relationships

### 🔹 Power BI Data Model

* star schema with multiple fact tables
* recruitment funnel analytics
* structured KPI framework

👉 The result is a **safe, realistic dataset** that behaves like production data.

---

# 🏗️ End-to-End Architecture

```text
Raw ATS Data
     ↓
Mapping Generation (IDs)
     ↓
Python Anonymization Pipelines
     ↓
Synthetic + Anonymized Dataset
     ↓
Power BI Data Model (Star Schema)
     ↓
Recruitment Analytics KPIs
```

---

# 📁 Repository Structure

```text
project/
├── applications/             # Processed application datasets (per client)
├── requisitions/             # Raw + reference requisition data
├── mappings/                 # Deterministic ID mapping tables
├── scripts/                  # Python pipelines
│   ├── anonymize_applications.py
│   ├── anonymize_requisitions.py
│   ├── generate_application_id_map.py
│   └── generate_candidate_id_map.py
├── documentation/
│   └── README.md
└── powerbi/
    └── data_model.pbix
```

---

# 📦 Mapping Files

Mapping files are used to ensure **deterministic anonymization** and consistency across datasets.

Due to their size and sensitivity, they are **not included in this repository**.

They can be generated using the provided scripts:

```bash
python scripts/generate_application_id_map.py
python scripts/generate_candidate_id_map.py
```

---

# 🔐 Anonymization Strategy

The solution focuses on **realism rather than simple masking**.

### Key Principles

* No real identifiers retained
* Deterministic anonymization (reproducible results)
* Business logic preserved

---

## 🔧 Techniques Used

### Identifiers

* Surrogate keys for requisitions, candidates, and applications
* Deterministic mapping tables ensure consistency

### People

* Names replaced using synthetic generation
* No emails or original values retained

### Job & Organization

* Titles mapped to reference taxonomy
* Organizational structures anonymized

### Compensation

* Salary ranges generated based on job grade

### Geography

* Derived from reference tables (country → city → currency)

### Free Text

* Fully removed to eliminate leakage risk

---

# 🧠 Synthetic Data Engine

The applications pipeline includes a **deterministic synthetic data engine**.

### Features

* Stable hashing (MD5) for reproducibility
* Multi-client simulation (C1–C4)
* Controlled randomness with consistent outputs

---

## 📊 Recruitment Funnel Simulation

```text
Applications → CV Review → Interviews → Offers → Hires → Starters
```

### Includes

* Stage progression probabilities
* Funnel drop-off modeling
* Offer acceptance logic
* Candidate reneging behavior

---

## ⏱ Temporal Realism

* Global timeline alignment
* Historical spread (~2 years)
* Hiring seasonality (spring/autumn peaks)
* Strict validation rules

👉 Ensures realistic analytics behavior

---

# 🧱 Data Model (Power BI)

## ⭐ Star Schema Design

### Fact Tables

* `fct_Requisitions` → job-level data
* `fct_Applications` → candidate-level data

### Dimensions

* `dim_Client`
* `dim_Job`
* `dim_Location`
* `dim_Source`
* `dim_Candidate`
* `dim_Person`
* `dim_App_Status`
* `dim_Requisition_Attributes`

---

## 🔗 Key Design Features

### Multi-Fact Model

* Separate grains for requisitions and applications
* Enables full recruitment funnel analysis

### Shared Dimensions

* Consistent filtering across fact tables

### Multiple Date Relationships

* Supports advanced time-based metrics
* (e.g. time to hire, time to offer)

---

# 📊 KPI Framework

The model includes a **comprehensive KPI layer** structured into categories:

---

### 📦 Volume KPIs

* Applications
* CV Reviewed
* Interviews
* Offers Made
* Hires
* Starters

---

### 📈 Conversion KPIs

* CV → Interview
* Interview → Offer
* Offer → Hire
* Initiated → Submitted

---

### ⚡ Speed KPIs

* Time to Offer
* Time to Post
* Time to Shortlist
* Aged Requisitions

---

### 🎯 Quality KPIs

* Offer Acceptance %
* Offer Dropout %
* Withdrawal %

---

### 💰 Financial KPIs

* Agency Usage %
* Source Mix %

---

### 🧠 KPI Design Philosophy

```text
Volume  → How much
Ratio   → How efficient
Speed   → How fast
Quality → How good
Cost    → How expensive
```

---

The Power BI model enables end-to-end recruitment analytics, including funnel conversion analysis, time-to-hire metrics, and sourcing performance evaluation.

---

# 🧪 Data Quality & Validation

The pipeline includes:

* deterministic mapping validation
* date sequence validation
* funnel integrity checks
* null handling and defensive defaults
* schema enforcement

---

# 🔄 Multi-Client Simulation

The project simulates multiple clients:

* C1, C2, C3, C4
* different data volumes
* consistent underlying logic

👉 Enables benchmarking and scalable analytics

---

# ⚙️ How to Run

## 1. Clone repository

```bash
git clone https://github.com/JedPawlowski/recruitment-analytics-anonymization-platform.git
```

## 2. Install dependencies

```bash
pip install pandas numpy faker
```

## 3. Add input files

Place in appropriate folders:

* requisitions_raw.xlsx
* applications_raw.csv
* reference tables

## 4. Run pipelines

```bash
python scripts/anonymize_requisitions.py
python scripts/anonymize_applications.py
```

---

# 🚀 What This Project Demonstrates

## 💡 Technical Skills

* data anonymization design
* synthetic data generation
* Python data engineering
* Power BI data modeling
* DAX KPI development

---

## 🧠 Analytical Thinking

* recruitment funnel modeling
* conversion analysis
* time-based metrics
* business-driven KPI design

---

## 🏗️ Architecture Skills

* star schema design
* multi-fact modeling
* deterministic pipelines
* reusable data frameworks

---

## ⭐ Summary

This project demonstrates how to build a complete analytics solution under real-world constraints — where sensitive data cannot be used directly.

By combining data anonymization, synthetic data generation, and a robust Power BI model, it enables realistic recruitment analytics while ensuring full data privacy.

It reflects an end-to-end approach to data: from raw inputs to business insights — with a strong focus on usability, scalability, and real-world applicability.

---
