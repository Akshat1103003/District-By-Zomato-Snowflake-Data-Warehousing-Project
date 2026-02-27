# 🏢 District Analytics — End-to-End Data Warehouse & Analytics Platform

<div align="center">

**A production-grade data warehouse built for [District by Zomato](https://www.zomato.com/district) — India's platform for dining experiences, movie ticketing, and live events.**

`AWS S3` · `Snowflake` · `Power BI` · `Medallion Architecture` · `SCD Type 2` · `Snowpipe` · `Dynamic Tables`

</div>

---

## 📌 Table of Contents

- [About the Project](#-about-the-project)
- [Architecture Overview](#-architecture-overview)
- [Tech Stack](#-tech-stack)
- [Data Pipeline Flow](#-data-pipeline-flow)
- [Snowflake Features Used](#%EF%B8%8F-snowflake-features-used)
- [Bronze Layer — Raw Ingestion](#-bronze-layer--raw-ingestion)
- [Silver Layer — Star Schema](#-silver-layer--star-schema)
- [Gold Layer — Analytics Data Marts](#-gold-layer--analytics-data-marts)
- [Slowly Changing Dimensions (SCDs)](#-slowly-changing-dimensions-scds)
- [Data Governance & Security](#-data-governance--security)
- [Pipeline Automation & Orchestration](#-pipeline-automation--orchestration)
- [Power BI Dashboards](#-power-bi-dashboards)
- [District Pass Subscription Analytics](#-district-pass-subscription-analytics)
- [Dataset Overview](#-dataset-overview)
- [Key Learnings & Design Decisions](#-key-learnings--design-decisions)
- [Complete Entity Inventory](#-complete-entity-inventory)
- [Repository Structure](#-repository-structure)
- [Getting Started](#-getting-started)
- [Author](#-author)

---

## 📖 About the Project

**District Analytics** is a comprehensive analytics engineering portfolio project that demonstrates building a modern data warehouse from scratch — from raw file ingestion through cloud storage to interactive BI dashboards.

The project models the data ecosystem for **District by Zomato**, India's integrated platform offering:
- 🍽️ **Dining Experiences** — Restaurant bookings and elevated culinary events
- 🎬 **Movie Ticketing** — Cinema ticket booking across multiplexes
- 🎵 **Events** — Concerts, comedy shows, sports screenings, and festivals
- 🎫 **District Pass** — Subscription loyalty program (launched January 2026) offering discounts on tickets, snacks, dining vouchers, and restaurant benefits

### What This Project Demonstrates

| Capability | Implementation |
|:-----------|:---------------|
| Cloud Data Integration | AWS S3 → Snowflake via IAM cross-account access |
| Automated Ingestion | Snowpipe with S3 event notifications (zero manual loading) |
| Dimensional Modeling | Star Schema with Fact + Dimension tables |
| Historical Tracking | SCD Type 2 on customer dimension with full version history |
| Automated Transformations | Dynamic Tables + Streams + Tasks + Stored Procedures (hybrid approach) |
| User-Defined Functions | Custom UDFs for age binning and loyalty tier calculation |
| Data Governance | RBAC (3 roles) + Dynamic Data Masking (4 policies) |
| Data Quality | Quarantine-based error handling with monitoring views |
| Subscription Analytics | Dedicated Pass dimension/fact tables + Gold data marts |
| Business Intelligence | Power BI dashboards via native Snowflake connector with MFA authentication |

---

## 🏗 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        SOURCE DATA (CSV Files)                         │
│        Realistic Indian market: Bollywood, UPI payments, Tier 1 cities │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │  Manual Upload
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     ☁️  AWS S3 (Data Lake)                              │
│         s3://district-analytics-raw/raw-folder/                        │
│         ├── 2025_raw_folder/                                           │
│         └── 2026_raw_folder/January_01/                                │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │  S3 Event Notification → SQS → Snowpipe
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  🥉 BRONZE LAYER (Raw Landing Zone)                               │  │
│  │  ┌──────────────────────────┐  ┌───────────────────────────────┐  │  │
│  │  │  DISTRICT_RAW_TABLE      │  │ Snowpipe + Stage + Stream     │  │  │
│  │  │             │  │ + File Format + Quarantine    │  │  │
│  │  │  All columns VARCHAR     │  │ + 4 Monitoring Views          │  │  │
│  │  └──────────────────────────┘  └───────────────────────────────┘  │  │
│  └───────────────────────────┬───────────────────────────────────────┘  │
│                              │                                          │
│                              │  Stream (CDC) ──► Task ──► Stored Proc   │
│                              │  Dynamic Tables (auto-refresh)           │
│                              ▼                                          │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  🥈 SILVER LAYER (Star Schema — Enterprise Data Warehouse)        │  │
│  │                                                                    │  │
│  │  Static Tables (4):                   Dynamic Tables (5):          │  │
│  │  ┌──────────────────┐                 ┌──────────────────────┐    │  │
│  │  │ DIM_CUSTOMER     │  SCD Type 2     │ DIM_MOVIE            │    │  │
│  │  │ DIM_DATE         │  SCD Type 0     │ DIM_VENUE            │    │  │
│  │  │ DIM_PASS_PLAN    │  Reference      │ FACT_BOOKINGS        │    │  │
│  │  │ DIM_PASS_BENEFIT │  Reference      │ FACT_PASS_SUBSCRIPTION│   │  │
│  │  │   _TYPE          │                 │ FACT_BENEFIT_         │    │  │
│  │  └──────────────────┘                 │   REDEMPTION          │    │  │
│  │                                        └──────────────────────┘    │  │
│  │  + 2 UDFs + 2 Stored Procedures + 1 Stream + 1 Task               │  │
│  └───────────────────────────┬───────────────────────────────────────┘  │
│                              │  Dynamic Tables (auto-refresh)           │
│                              ▼                                          │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  🥇 GOLD LAYER (9 Analytics Data Marts — All Dynamic Tables)      │  │
│  │                                                                    │  │
│  │  General Analytics (7):              Pass Analytics (2):           │  │
│  │  ┌────────────────────────┐          ┌────────────────────────┐   │  │
│  │  │ GOLD_BOOKING_TRENDS    │          │ GOLD_PASS_DAILY_KPIS   │   │  │
│  │  │ GOLD_CUSTOMER_COHORTS  │          │ GOLD_PASS_PERFORMANCE_ │   │  │
│  │  │ GOLD_CUSTOMER_METRICS  │          │   MART                 │   │  │
│  │  │ GOLD_MOVIE_ANALYTICS   │          └────────────────────────┘   │  │
│  │  │ GOLD_REVENUE_DAILY     │                                       │  │
│  │  │ GOLD_VENUE_DAILY_      │                                       │  │
│  │  │   METRICS              │                                       │  │
│  │  │ GOLD_VENUE_PERFORMANCE │                                       │  │
│  │  └────────────────────────┘                                       │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│                     ❄️  SNOWFLAKE  (DISTRICT_ANALYTICS_DB)              │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │  Native Snowflake Connector
                               │  Import Mode + Duo MFA Authentication
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     📊 POWER BI DASHBOARDS                              │
│  Customer Intelligence │ Revenue Performance │ Venue & Movie Analytics  │
│  Booking Trends        │ Cohort Retention    │ Pass Subscription KPIs   │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🛠 Tech Stack

| Layer | Technology | Role |
|:------|:-----------|:-----|
| **Data Lake** | AWS S3 | Raw file storage with organized year/month folder structure |
| **Ingestion** | Snowpipe + S3 Event Notifications | Automated, event-driven micro-batch loading via SQS |
| **Warehouse** | Snowflake | Core DWH with Dynamic Tables, Streams, Tasks, Stored Procedures, UDFs |
| **Transformation** | Snowflake-native SQL | Bronze → Silver → Gold processing with hybrid SCD strategies |
| **Governance** | Snowflake RBAC + Masking Policies | PII protection via 4 dynamic masking policies across 3 custom roles |
| **Visualization** | Microsoft Power BI | Interactive dashboards connected via native Snowflake connector |
| **Authentication** | AWS IAM + Snowflake MFA (Duo Mobile) | Secure cross-platform access with multi-factor authentication |

---

## 🔄 Data Pipeline Flow

```
CSV Upload to S3
       │
       ▼
S3 Event Notification ──► SQS Queue ──► Snowpipe (AUTO_INGEST)
                                              │
                                              ▼
                                    ┌─────────────────┐
                                    │  BRONZE LAYER    │
                                    │  (Raw VARCHAR)   │
                                    └────────┬────────┘
                                             │
                          ┌──────────────────┼──────────────────┐
                          ▼                  ▼                  ▼
                   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
                   │   Stream    │   │  Dynamic    │   │  Dynamic    │
                   │   (CDC)     │   │  Tables     │   │  Tables     │
                   │      │      │   │             │   │             │
                   │      ▼      │   │             │   │             │
                   │   Task      │   │             │   │             │
                   │   (5 min)   │   │             │   │             │
                   │      │      │   │             │   │             │
                   │      ▼      │   │             │   │             │
                   │  Stored     │   │             │   │             │
                   │  Procedure  │   │             │   │             │
                   └──────┬──────┘   └──────┬──────┘   └──────┬──────┘
                          ▼                  ▼                  ▼
                   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
                   │ DIM_CUSTOMER│   │ DIM_MOVIE   │   │FACT_BOOKINGS│
                   │ (SCD Type 2)│   │ DIM_VENUE   │   │FACT_PASS_*  │
                   │             │   │ DIM_DATE    │   │             │
                   └─────────────┘   │ DIM_PASS_*  │   └─────────────┘
                          │          └─────────────┘          │
                          └──────────────────┬────────────────┘
                                             ▼
                                    ┌─────────────────┐
                                    │   GOLD LAYER     │
                                    │  9 Data Marts    │
                                    │  (Dynamic Tables)│
                                    └────────┬────────┘
                                             ▼
                                    ┌─────────────────┐
                                    │   POWER BI       │
                                    │  Import Mode     │
                                    │  + Duo MFA Auth  │
                                    └─────────────────┘
```

---

## ❄️ Snowflake Features Used

This project leverages a wide range of Snowflake-native features to build a production-grade, automated data warehouse — avoiding external tools wherever Snowflake provides native capabilities.

### Storage & Ingestion Objects

| Feature | Object(s) | Purpose |
|:--------|:----------|:--------|
| **Storage Integration** | `S3_DISTRICT_INTEGRATION` | Secure, IAM-based cross-account access between AWS S3 and Snowflake without embedding credentials |
| **External Stage** | `DISTRICT_STAGE` | Points to the S3 bucket path via the storage integration, enabling Snowflake to read files from S3 |
| **File Format** | `CSV_FILE_FORMAT` | Defines CSV parsing rules — comma delimiter, header skip, field enclosure, error tolerance |
| **Snowpipe** | `DISTRICT_BRONZE_SNOWPIPE` | Event-driven auto-ingestion with `AUTO_INGEST = TRUE`; listens for S3 event notifications via SQS and loads new files within seconds |

### Change Data Capture & Automation

| Feature | Object(s) | Purpose |
|:--------|:----------|:--------|
| **Stream** | `DISTRICT_RAW_TABLE_STREAM` | Tracks INSERT/UPDATE/DELETE operations on the Bronze table for Change Data Capture (CDC); enables incremental processing of only new/changed rows |
| **Task** | `LOAD_DIM_CUSTOMER_TASK` | Scheduled every 5 minutes with `SYSTEM$STREAM_HAS_DATA()` guard — only executes the merge procedure when new data actually exists in the stream |
| **Stored Procedures** | `INIT_DIM_CUSTOMER()`, `LOAD_DIM_CUSTOMER()` | `INIT_DIM_CUSTOMER` performs one-time historical backfill; `LOAD_DIM_CUSTOMER` runs the incremental SCD Type 2 MERGE logic (expire old versions, insert new versions) |
| **User-Defined Functions** | `BIN_AGE_GROUP(VARCHAR)`, `CALCULATE_LOYALTY_TIER(NUMBER, NUMBER)` | `BIN_AGE_GROUP` converts raw age strings into analytical bins (18-25, 26-35, etc.); `CALCULATE_LOYALTY_TIER` computes Platinum/Gold/Silver/Bronze tiers based on lifetime value and booking frequency |

### Table Types Used

| Table Type | Where Used | Why This Type |
|:-----------|:-----------|:--------------|
| **Static Tables** | `DIM_DATE`, `DIM_CUSTOMER`, `DIM_PASS_PLAN`, `DIM_PASS_BENEFIT_TYPE` | `DIM_DATE` is immutable (SCD Type 0); `DIM_CUSTOMER` requires imperative SCD Type 2 MERGE logic that Dynamic Tables cannot express; Pass dimensions are reference data loaded once |
| **Dynamic Tables** | `DIM_MOVIE`, `DIM_VENUE`, `FACT_BOOKINGS`, `FACT_PASS_SUBSCRIPTION`, `FACT_BENEFIT_REDEMPTION`, all 9 Gold tables | Declarative, auto-refreshing tables that Snowflake maintains automatically; ideal for SCD Type 1 dimensions and fact tables where transformations can be expressed as a single SELECT statement |

### Data Governance Features

| Feature | Object(s) | Purpose |
|:--------|:----------|:--------|
| **Custom Roles (RBAC)** | `DATA_ENGINEER_ROLE`, `ANALYST_ROLE`, `SUPPORT_ROLE` | Role-based access control simulating real-world organizational access patterns with different permission levels |
| **Dynamic Data Masking** | `EMAIL_MASK`, `PHONE_MASK`, `NAME_MASK`, `FINANCIAL_MASK` | Column-level masking policies that use `CURRENT_ROLE()` to dynamically determine data visibility at query time — same table, different views per role |

---

## 🥉 Bronze Layer — Raw Ingestion

The Bronze layer is the **immutable raw data landing zone**. All columns are stored as `VARCHAR` to preserve complete source fidelity (schema-on-read approach). Data quality issues are captured in quarantine rather than silently dropped.

### Bronze Layer Objects

| Object | Type | Purpose |
|:-------|:-----|:--------|
| `DISTRICT_RAW_TABLE` | Table | Raw landing table (~175,000 rows, all VARCHAR columns) |
| `DISTRICT_STAGE` | External Stage | Points to S3 via storage integration |
| `CSV_FILE_FORMAT` | File Format | CSV parsing configuration (delimiter, header skip, enclosure) |
| `DISTRICT_BRONZE_SNOWPIPE` | Pipe | Auto-ingest pipe triggered by S3 event notifications via SQS |
| `DISTRICT_RAW_TABLE_STREAM` | Stream | CDC tracking on raw table for downstream Silver layer processing |

### Data Quality — Quarantine System

Instead of silently dropping invalid records, the pipeline routes them to `DISTRICT_RAW_QUARANTINE` with full error metadata:

| Monitoring View | What It Tracks |
|:----------------|:---------------|
| `VW_QUARANTINE_SUMMARY` | Overall quarantine status and counts |
| `VW_QUARANTINE_TOP_ERRORS` | Most common data quality issues |
| `VW_QUARANTINE_BY_FILE` | Error distribution by source file |
| `VW_QUARANTINE_AGING` | How long quarantined records remain unresolved |

**Remediation**: `SP_REMEDIATE_QUARANTINE` stored procedure enables fix-and-reload workflows for quarantined records.

---

## 🥈 Silver Layer — Star Schema

The Silver layer transforms raw Bronze data into a **Star Schema dimensional model** with cleansed, typed, and conformed tables. A **hybrid transformation approach** uses Dynamic Tables for simple refreshes and Streams + Tasks + Stored Procedures for complex SCD Type 2 processing.

### Star Schema Design

```
                              ┌──────────────────┐
                              │    dim_date       │
                              │    SCD Type 0     │
                              │    Static Table   │
                              │                   │
                              └────────┬─────────┘
                                       │
┌──────────────────┐          ┌────────┴─────────┐          ┌──────────────────┐
│  dim_customer    │──────────│  fact_bookings   │──────────│  dim_movie       │
│  SCD Type 2      │          │  Dynamic Table   │          │  SCD Type 1      │
│  Stream + Task   │          │                  │          │  Dynamic Table   │
│  + Procedures    │          └────────┬─────────┘          │                  │
│  + 2 UDFs        │                   │                    └──────────────────┘
│                  │          ┌────────┴─────────┐
└──────────────────┘          │   dim_venue      │
                              │   SCD Type 1     │
                              │   Dynamic Table  │
┌──────────────────┐          │                  │
│ dim_pass_plan    │          └──────────────────┘
│ dim_pass_benefit │
│   _type          │          ┌──────────────────────────────┐
│ Static Tables    │──────────│ fact_pass_subscription       │
└──────────────────┘          │ fact_benefit_redemption      │
                              │ Dynamic Tables               │
                              └──────────────────────────────┘
```

### Silver Layer — Complete Object Inventory

**Tables (4 Static):**

| Table | SCD Type | Mechanism | Key Details |
|:------|:---------|:----------|:------------|
| `DIM_CUSTOMER` | **Type 2 (Historical)** | Stream → Task → Stored Procedure | Tracks city changes with `VERSION_NUMBER`, `START_DATE`, `END_DATE`, `IS_CURRENT`; uses `BIN_AGE_GROUP()` and `CALCULATE_LOYALTY_TIER()` UDFs; ~12,500 unique customers |
| `DIM_DATE` | **Type 0 (Static)** | Pre-populated, immutable | Calendar dimension with year, quarter, month, week, day, is_weekend; ~4,018 rows |
| `DIM_PASS_PLAN` | **Reference** | Static reference data | Pass plan definitions — plan name, price, validity days, benefit limits per category (movie/dining/snack) |
| `DIM_PASS_BENEFIT_TYPE` | **Reference** | Static reference data | Benefit type catalog — movie free tickets, dining vouchers, snack discounts, restaurant instant discounts |

**Dynamic Tables (5 Auto-Refresh):**

| Table | Refreshes From | Key Details |
|:------|:---------------|:------------|
| `DIM_MOVIE` | Bronze (SCD Type 1) | Movie name, genre, language, duration, certification, base price; ~150 movies |
| `DIM_VENUE` | Bronze (SCD Type 1) | Venue name, type, city, state, capacity, rating, lat/long; ~991 venues |
| `FACT_BOOKINGS` | Bronze + Silver dims | Core fact with **category standardization** (`"Movie"` → `"Movies"`, `"Food Festival"` → `"Dining"`); ~175,000 rows |
| `FACT_PASS_SUBSCRIPTION` | Bronze + Silver dims | Pass subscription transactions — purchase/start/end dates, plan key, benefit usage counters |
| `FACT_BENEFIT_REDEMPTION` | Bronze + Silver dims | Benefit redemption events — benefit type, original/discount amounts, usage sequence |

**Functions (2 UDFs):**

| Function | Signature | Purpose |
|:---------|:----------|:--------|
| `BIN_AGE_GROUP` | `(VARCHAR) → VARCHAR` | Converts raw age to bins: `'18-25'`, `'26-35'`, `'36-45'`, `'46-55'`, `'56+'`, `'Unknown'` |
| `CALCULATE_LOYALTY_TIER` | `(NUMBER, NUMBER) → VARCHAR` | Computes tier from (lifetime_value, booking_count): `'Platinum'` (₹50K+ OR 20+ bookings), `'Gold'`, `'Silver'`, `'Bronze'` |

**Stored Procedures (2):**

| Procedure | Purpose |
|:----------|:--------|
| `INIT_DIM_CUSTOMER()` | One-time backfill — creates Version 1 for all existing customers |
| `LOAD_DIM_CUSTOMER()` | Incremental SCD Type 2 MERGE — expires old versions, inserts new versions |

**Automation Objects:**

| Object | Type | Configuration |
|:-------|:-----|:-------------|
| `DISTRICT_RAW_TABLE_STREAM` | Stream | CDC on Bronze table with `SHOW_INITIAL_ROWS = TRUE` |
| `LOAD_DIM_CUSTOMER_TASK` | Task | `SCHEDULE = '5 MINUTE'` with `WHEN SYSTEM$STREAM_HAS_DATA()` guard |

---

## 🥇 Gold Layer — Analytics Data Marts

**Nine Dynamic Tables** provide pre-aggregated, dashboard-ready metrics with auto-refresh from Silver:

### General Analytics (7 Tables)

| Gold Table | Data Mart | Key Pre-Computed Metrics |
|:-----------|:----------|:------------------------|
| `GOLD_CUSTOMER_METRICS` | Customer Analytics | RFM status (Active/At Risk/Dormant/Churned), lifetime revenue, booking frequency, cancellation rate |
| `GOLD_REVENUE_DAILY` | Finance | Daily revenue, 7-day & 30-day moving averages (window functions), category breakdown |
| `GOLD_VENUE_PERFORMANCE` | Operations | Revenue per venue, capacity utilization, ratings, performance rankings |
| `GOLD_VENUE_DAILY_METRICS` | Operations (Temporal) | Daily venue performance trends, time-series analysis |
| `GOLD_MOVIE_ANALYTICS` | Content | Revenue per movie, popularity rank, demographic breakdowns by genre |
| `GOLD_BOOKING_TRENDS` | Marketing | Multi-dimensional aggregation: time × category × gender × age group × loyalty tier × city |
| `GOLD_CUSTOMER_COHORTS` | Retention | Cohort retention rates by acquisition month, months-since-first, revenue per customer |

### Pass Subscription Analytics (2 Tables)

| Gold Table | Data Mart | Key Pre-Computed Metrics |
|:-----------|:----------|:------------------------|
| `GOLD_PASS_PERFORMANCE_MART` | Pass Customer Analytics | Per-subscriber: total discounts, benefit utilization % (movie/dining/snack), net revenue impact, ROI |
| `GOLD_PASS_DAILY_KPIS` | Pass Operational KPIs | Daily: new subscriptions, redemptions, subscriber vs non-subscriber revenue, benefit breakdown |

---

## 🔄 Slowly Changing Dimensions (SCDs)

Three SCD types are implemented based on each table's business requirements:

### SCD Strategy Matrix

| SCD Type | Strategy | Tables | Mechanism | Rationale |
|:---------|:---------|:-------|:----------|:----------|
| **Type 0** | Retain Original | `dim_date` | Static table (loaded once) | Calendar dates are immutable — attributes never change |
| **Type 1** | Overwrite | `dim_movie`, `dim_venue`, `dim_pass_plan`, `dim_pass_benefit_type` | Dynamic Tables / Static tables | Current-state accuracy is sufficient; no need to track historical attribute changes |
| **Type 2** | Add New Row | `dim_customer` | Stream + Task + Stored Procedure | Customer city changes are analytically significant for cohort analysis and customer journey tracking |

### SCD Type 2 Lifecycle Example (dim_customer)

When a customer moves from Mumbai to Delhi:

| Step | Action | VERSION | CITY | START_DATE | END_DATE | IS_CURRENT |
|:-----|:-------|:--------|:-----|:-----------|:---------|:-----------|
| 1 | Initial load | 1 | Mumbai | 2024-10-15 | NULL | TRUE |
| 2 | Stream captures city change | — | — | — | — | — |
| 3 | Task triggers → Procedure expires old | 1 | Mumbai | 2024-10-15 | 2025-06-01 | **FALSE** |
| 4 | Procedure inserts new version | 2 | Delhi | 2025-06-01 | NULL | **TRUE** |

**Query patterns:**
- Current state: `WHERE IS_CURRENT = TRUE`
- Point-in-time: `WHERE START_DATE <= target_date AND (END_DATE > target_date OR END_DATE IS NULL)`

### Design Decision: dim_venue Migration

Originally `dim_venue` was SCD Type 2 (Streams + Tasks). It was **pragmatically migrated to a Dynamic Table (SCD Type 1)** after analysis revealed that venue attribute changes didn't require historical tracking. This reduced pipeline complexity while maintaining data accuracy — embodying the principle: *choose the simplest approach that meets business requirements.*

---

## 🔐 Data Governance & Security

### Role-Based Access Control (RBAC)

| Role | Access Level | Use Case | Data Visibility |
|:-----|:-------------|:---------|:----------------|
| `DATA_ENGINEER_ROLE` | Full (SELECT, INSERT, UPDATE, DELETE) | ETL/ELT pipeline development | **Unmasked** — full PII access |
| `ANALYST_ROLE` | Read-only (SELECT) | Business analytics & reporting | **Masked** — partial email, last 4 phone digits, first name only, NULL financials |
| `SUPPORT_ROLE` | Read-only (SELECT) | Customer service operations | **Partially masked** — enough to identify customers |

### Dynamic Data Masking Policies

| Policy | Applied To | Engineer View | Analyst/Support View |
|:-------|:-----------|:-------------|:---------------------|
| `EMAIL_MASK` | USER_EMAIL | `akshat@gmail.com` | `ak****@g****.com` |
| `PHONE_MASK` | USER_PHONE | `+91-9876543210` | `******3210` |
| `NAME_MASK` | CUSTOMER_NAME | `Rajesh Kumar` | `Rajesh K.` |
| `FINANCIAL_MASK` | TOTAL_AMOUNT, PAYMENT_AMOUNT | `₹1,250.00` | `NULL` |

> **Why warehouse-level masking?** Masking at the Snowflake level (using `CURRENT_ROLE()`) provides a single enforcement point regardless of which tool queries the data — Power BI, SQL worksheet, or programmatic access. This is the approach mandated by GDPR, CCPA, and India's DPDP Act.

---

## ⚡ Pipeline Automation & Orchestration

The entire pipeline operates with **zero manual intervention** after CSV upload to S3:

| Step | Component | Trigger | Action |
|:-----|:----------|:--------|:-------|
| 1 | **S3 Event Notification** | New file lands in S3 | Sends message to SQS queue |
| 2 | **Snowpipe** (`DISTRICT_BRONZE_SNOWPIPE`) | SQS notification | Auto-loads CSV into Bronze within seconds |
| 3 | **Stream** (`DISTRICT_RAW_TABLE_STREAM`) | DML on Bronze table | Captures new rows as CDC events |
| 4 | **Task** (`LOAD_DIM_CUSTOMER_TASK`) | Every 5 min + `STREAM_HAS_DATA()` = TRUE | Calls `LOAD_DIM_CUSTOMER()` procedure |
| 5 | **Stored Procedure** (`LOAD_DIM_CUSTOMER`) | Called by Task | SCD Type 2 MERGE — expire old, insert new versions |
| 6 | **Silver Dynamic Tables** | Upstream changes detected | `DIM_MOVIE`, `DIM_VENUE`, `FACT_BOOKINGS`, `FACT_PASS_*` auto-refresh |
| 7 | **Gold Dynamic Tables** | Silver changes detected | All 9 data marts auto-refresh |
| 8 | **Power BI** | Scheduled/manual refresh | Dashboards reflect updated data |

### Why `SYSTEM$STREAM_HAS_DATA()` Matters

The Task doesn't blindly run every 5 minutes — the guard ensures the procedure only executes when **actual new data exists**. This prevents wasted compute credits, accidental stream consumption without data, and unnecessary SCD Type 2 version creation.

---

## 📊 Power BI Dashboards

### Connection Architecture

| Setting | Value | Why |
|:--------|:------|:----|
| **Connector** | Power BI Native Snowflake Connector | Supports query pushdown, handles Snowflake authentication natively |
| **Connection Mode** | **Import** | Pre-loads data into Power BI's in-memory engine for **sub-second visual interactions**; enables offline demo capability for portfolio presentations |
| **Authentication** | Username/Password + **Duo Mobile MFA Push** | Multi-factor authentication required; Duo Mobile added after Windows Hello proved incompatible with Snowflake's programmatic MFA |
| **Schema** | `GOLD_LAYER` only | Bronze and Silver schemas are never exposed to the BI layer |

> **Why Import Mode over DirectQuery?** For portfolio demonstrations, Import mode provides sub-second interactions regardless of network latency. Gold layer data already has a 25-minute refresh lag, so real-time DirectQuery adds complexity without benefit. Import also enables offline demos without active Snowflake connectivity.

### Dashboard Suite

| Dashboard | Source Table(s) | Key Visualizations |
|:----------|:---------------|:-------------------|
| **Customer Intelligence** | `GOLD_CUSTOMER_METRICS` | RFM segmentation matrix, customer status distribution, LTV histogram, demographic breakdown |
| **Revenue Performance** | `GOLD_REVENUE_DAILY` | Revenue with 7/30-day moving averages, category donut, top venues bar chart |
| **Venue Analytics** | `GOLD_VENUE_PERFORMANCE` + `GOLD_VENUE_DAILY_METRICS` | Venue scorecard, geographic map, capacity utilization, daily trends |
| **Movie Analytics** | `GOLD_MOVIE_ANALYTICS` | Top movies by revenue/ratings, genre popularity, demographic appeal |
| **Booking Trends** | `GOLD_BOOKING_TRENDS` | Day-of-week patterns, seasonal trends, demographic slice-and-dice |
| **Cohort Retention** | `GOLD_CUSTOMER_COHORTS` | Retention heatmap, retention curves, revenue per customer by cohort age |
| **Pass Subscription** | `GOLD_PASS_PERFORMANCE_MART` + `GOLD_PASS_DAILY_KPIS` | Adoption rate, benefit utilization, subscriber vs non-subscriber revenue, ROI |

---

## 🎫 District Pass Subscription Analytics

The **District Pass** is a subscription loyalty program (January 2026) offering discounts on tickets, snacks, dining, and restaurants.

### Pass Data Architecture

```
SILVER (Granular):                          GOLD (Pre-Aggregated):
├── DIM_PASS_PLAN (plan definitions)        ├── GOLD_PASS_PERFORMANCE_MART
├── DIM_PASS_BENEFIT_TYPE (benefit catalog)  │   (per-subscriber analytics)
├── FACT_PASS_SUBSCRIPTION (who bought)     └── GOLD_PASS_DAILY_KPIS
└── FACT_BENEFIT_REDEMPTION (what redeemed)      (daily operational trends)
```

### Analytics Enabled

| Business Question | Source |
|:------------------|:-------|
| Total Pass revenue & subscriber count | `FACT_PASS_SUBSCRIPTION` |
| Cities with highest Pass adoption | `FACT_PASS_SUBSCRIPTION` + `DIM_CUSTOMER` |
| Most utilized benefit category | `FACT_BENEFIT_REDEMPTION` + `DIM_PASS_BENEFIT_TYPE` |
| % of customers with Pass | `FACT_PASS_SUBSCRIPTION` + `DIM_CUSTOMER` |
| Pass ROI (revenue vs discounts) | `GOLD_PASS_PERFORMANCE_MART` |
| Subscriber vs non-subscriber behavior | `GOLD_PASS_DAILY_KPIS` |

---

## 📋 Dataset Overview

| Metric | Value |
|:-------|:------|
| **Total Records** | ~175,000 bookings |
| **Unique Customers** | ~12,500 |
| **Venues** | 991 |
| **Movies** | 150 (Bollywood + Hollywood) |
| **Date Range** | October 2024 — April 2026 |
| **Categories** | Dining, Movies, Events |
| **Geography** | Indian Tier 1 cities (Mumbai, Delhi, Bangalore, etc.) |
| **Payments** | UPI (PhonePe, GPay, Paytm), Credit/Debit Card, Wallets |
| **Pass Subscriptions** | ~1,000 (Jan–Mar 2026) |

---

## 💡 Key Learnings & Design Decisions

| Decision | Context | Outcome |
|:---------|:--------|:--------|
| **Hybrid Transformation** | Dynamic Tables for SCD 0/1; Streams + Tasks for SCD 2 | More maintainable — Dynamic Tables can't express MERGE logic |
| **dim_venue Migration** | SCD Type 2 → Dynamic Table (Type 1) | Reduced complexity; historical venue tracking wasn't needed |
| **Import Mode** | Power BI connection mode selection | Sub-second interactions + offline demo capability |
| **Pre-Aggregation in Snowflake** | Moving averages, retention rates in Gold layer | Keeps Power BI lightweight; shifts compute to warehouse |
| **Quarantine over Silent Drops** | Invalid records → quarantine table | Full data lineage and auditability |
| **UDFs for Reusable Logic** | Age binning + loyalty tier as SQL functions | Consistent logic in both initial and incremental loads |
| **Duo Mobile for MFA** | Windows Hello incompatible with programmatic MFA | Power BI native connector handles Duo push notifications |
| **`STREAM_HAS_DATA()` Guard** | Prevents wasteful Task execution | Only runs when actual new data exists |

---

## 📦 Complete Entity Inventory

### Bronze Layer (5 objects + quarantine system)

| Object | Type |
|:-------|:-----|
| `DISTRICT_RAW_TABLE` | Table |
| `DISTRICT_STAGE` | External Stage |
| `CSV_FILE_FORMAT` | File Format |
| `DISTRICT_BRONZE_SNOWPIPE` | Pipe |
| `DISTRICT_RAW_TABLE_STREAM` | Stream |

### Silver Layer (9 tables + 6 supporting objects)

| Object | Type |
|:-------|:-----|
| `DIM_CUSTOMER`, `DIM_DATE`, `DIM_PASS_PLAN`, `DIM_PASS_BENEFIT_TYPE` | Static Tables (4) |
| `DIM_MOVIE`, `DIM_VENUE`, `FACT_BOOKINGS`, `FACT_PASS_SUBSCRIPTION`, `FACT_BENEFIT_REDEMPTION` | Dynamic Tables (5) |
| `BIN_AGE_GROUP`, `CALCULATE_LOYALTY_TIER` | Functions / UDFs (2) |
| `INIT_DIM_CUSTOMER`, `LOAD_DIM_CUSTOMER` | Stored Procedures (2) |
| `LOAD_DIM_CUSTOMER_TASK` | Task (1) |

### Gold Layer (9 Dynamic Tables)

| Object | Category |
|:-------|:---------|
| `GOLD_BOOKING_TRENDS`, `GOLD_CUSTOMER_COHORTS`, `GOLD_CUSTOMER_METRICS`, `GOLD_MOVIE_ANALYTICS`, `GOLD_REVENUE_DAILY`, `GOLD_VENUE_DAILY_METRICS`, `GOLD_VENUE_PERFORMANCE` | General Analytics (7) |
| `GOLD_PASS_DAILY_KPIS`, `GOLD_PASS_PERFORMANCE_MART` | Pass Analytics (2) |

### Security (3 roles + 4 masking policies)

| Object | Type |
|:-------|:-----|
| `DATA_ENGINEER_ROLE`, `ANALYST_ROLE`, `SUPPORT_ROLE` | Roles (3) |
| `EMAIL_MASK`, `PHONE_MASK`, `NAME_MASK`, `FINANCIAL_MASK` | Masking Policies (4) |

> **Total Snowflake Objects: 36**

---

## 📁 Repository Structure

```
district-analytics/
│
├── README.md
├── docs/
│   └── District_Analytics_Documentation.docx
│
├── data/
│   ├── district_transactions_2025.csv
│   └── district_transactions_2026.csv
│
├── snowflake/
│   ├── 00_setup/
│   │   ├── 01_database_schemas.sql
│   │   ├── 02_storage_integration.sql
│   │   ├── 03_stage_fileformat.sql
│   │   └── 04_snowpipe.sql
│   │
│   ├── 01_bronze/
│   │   ├── 01_raw_table.sql
│   │   ├── 02_quarantine_table.sql
│   │   └── 03_quarantine_views.sql
│   │
│   ├── 02_silver/
│   │   ├── 01_dim_date.sql
│   │   ├── 02_dim_movie.sql
│   │   ├── 03_dim_venue.sql
│   │   ├── 04_dim_customer_scd2.sql           # Stream + Task + Procedures + UDFs
│   │   ├── 05_dim_pass_plan.sql
│   │   ├── 06_dim_pass_benefit_type.sql
│   │   ├── 07_fact_bookings.sql
│   │   ├── 08_fact_pass_subscription.sql
│   │   └── 09_fact_benefit_redemption.sql
│   │
│   ├── 03_gold/
│   │   ├── 01_gold_customer_metrics.sql
│   │   ├── 02_gold_revenue_daily.sql
│   │   ├── 03_gold_venue_performance.sql
│   │   ├── 04_gold_venue_daily_metrics.sql
│   │   ├── 05_gold_movie_analytics.sql
│   │   ├── 06_gold_booking_trends.sql
│   │   ├── 07_gold_customer_cohorts.sql
│   │   ├── 08_gold_pass_performance_mart.sql
│   │   └── 09_gold_pass_daily_kpis.sql
│   │
│   └── 04_governance/
│       ├── 01_roles_rbac.sql
│       └── 02_masking_policies.sql
│
├── powerbi/
│   ├── District_Analytics.pbix
│   └── screenshots/
│
└── assets/
    └── architecture_diagram.png
```

---

## 🚀 Getting Started

### Prerequisites

- **AWS Account** with S3 access
- **Snowflake Account** (Standard or Enterprise edition)
- **Power BI Desktop** (free from Microsoft)

### Setup Steps

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/district-analytics.git
   cd district-analytics
   ```

2. **AWS S3 Setup** — Create bucket, upload CSVs, configure IAM role with trust policy for Snowflake

3. **Snowflake Setup** — Execute SQL scripts in order:
   ```
   00_setup/ → 01_bronze/ → 02_silver/ → 03_gold/ → 04_governance/
   ```

4. **Snowpipe Activation** — Get SQS ARN from `SHOW PIPES`, configure S3 event notification

5. **Power BI** — Get Data → Snowflake → `GOLD_LAYER` schema → Import mode → Authenticate with Duo MFA

---

## 👤 Author

**Akshat**
- Role: Analytics Engineer
- Stack: AWS · Snowflake · Power BI · SQL · Data Warehousing · Microsoft Fabric

---

<div align="center">

**⭐ If this project helped you understand modern data warehouse architecture, please star the repo!**

*Built with 2026 data engineering best practices*

</div>
