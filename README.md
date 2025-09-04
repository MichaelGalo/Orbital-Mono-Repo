# Orbital

An end-to-end data engineering capstone that demonstrates a modern data lakehouse pipeline using lightweight, file-based tooling with DuckDB and DuckLake to produce a production-capable data product.

Orbital, simplifying the astronomical.

## Table of contents

- [Overview](#overview)
- [Features](#features)
- [Data Sources](#data-sources)
- [Architecture](#architecture)
- [Core technologies](#core-technologies)
- [Data Schemas](#data-schemas)
- [Running the project locally](#running-the-project-locally)


## Overview

This project is a production-minded capstone that ingests public datasets related to Space Exploration, validates and standardizes them, and exposes clean, analysis/web-ready tables for downstream use. The primary goals are:

- Demonstrate a modern lakehouse data architecture and medallion ELT patterns
- Use fast, in-memory analytical engines (DuckDB + DuckLake) for exploration and testing
- Provide repeatable pipelines and clear data quality checks that update automatically each day
- Produce datasets that are ready for analytics, visualization, and/or loading into a cloud warehouse
- Create clean, reusable code for future endeavors

The repository contains pipeline scripts, utilities, orchestration, API, and when run, a catalog of datasets arranged into raw, staged, and cleaned layers in the `data/` directory. The current .parquet files are gitignored for privacy.

## Data Sources

- Astronaut Data:The Space Devs Launch Library 2 API
- Exoplanet Data: Astroquery SDK (Community maintained access to NASA's Exoplanet Database)
- Space Weather Notifications: NASA DONKI (Database of Notifications, Knowledge and Information) API
- Hero Image: NASA APOD (Astronomy Picture of the Day) API

## Features

- MinIO data lake for storage (easily adapted to S3)
- Layered medallion catalog architecture (Raw → Staged → Clean)
- File-based analytics using DuckDB and DuckLake for fast queries
- Lightweight pipeline scripts for ingestion, quality checks, and lakehouse catalog sync
- REST API programmatic data access for serving cleaned data
- Data Quality Tests with custom python & SQL
- Unit tests with pytest and structured logging
- Orchestration with Prefect for daily updated data

## Architecture

![System Diagram](screenshots/Orbital_Architecture.png)

## Tech Stack

| Category | Technology | Purpose |
|----------|------------|---------|
| **Object Storage** | ![MinIO](https://img.shields.io/badge/MinIO-C72E49?style=flat-square&logo=minio&logoColor=white) | S3-compatible staging storage |
| **Data Lakehouse** | ![Ducklake](https://img.shields.io/badge/Ducklake-2E7D32?style=flat-square&logo=duckdb&logoColor=white) | Data lakehouse Catalog |
| **Database Operations** | ![DuckDB](https://img.shields.io/badge/DuckDB-FF6F00?style=flat-square&logo=duckdb&logoColor=white) | Analytical database for operations and querying |
| **Programming & Utilities** | ![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white) | Data ingestion, utilities, and scripting |
| **Data Transformation** | ![SQL](https://img.shields.io/badge/SQL-0066CC?style=flat-square&logo=sql&logoColor=white) | SQL-based transformations|
| **Data Processing** | ![Polars](https://img.shields.io/badge/Polars-5A4FCF?style=flat-square&logo=rust&logoColor=white) | DataFrame manipulation |
| **Testing** | ![pytest](https://img.shields.io/badge/pytest-009FE3?style=flat-square&logo=pytest&logoColor=white) | Automated testing |
| **Data Quality** | ![SQL](https://img.shields.io/badge/SQL-0066CC?style=flat-square&logo=sql&logoColor=white) | Data quality checks using SQL |
| **Orchestration** | ![Prefect](https://img.shields.io/badge/Prefect-3E4B99?style=flat-square&logo=prefect&logoColor=white) | Workflow automation and scheduling |
| **CI/CD** | ![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=flat-square&logo=githubactions&logoColor=white) | Continuous integration and deployment |

## Data Schemas

This project follows a medallion architecture with three primary layers:

- Raw (Bronze)
	- Immutable ingested data as originally fetched from APIs that are time-stamped
	- Stored in MinIO with catalog .parquet stored in `data/RAW/`

- Staged (Silver)
	- Standardized and validated records, ready for transformation
	- Stored in `data/STAGED/`

- Cleaned (Gold)
	- Business-ready, deduplicated tables suitable for analytics or web services
	- Stored in `data/CLEANED/`

Typical flow: ingest -> standardize/transform -> quality test -> clean -> serve

## Running the Project Locally

1. **Clone the repository**:
   ```bash
   git clone https://github.com/your-username/your-repo.git
   cd your-repo
   ```

2. **Set up a virtual environment**:
   ```bash
   uv init
   uv sync
   ```

3. **Setup MinIO**:
	```bash
	docker compose up -d
	```

4. **Create MinIO Bucket**:
	- Create a bucket called `orbital`

5. **Run the Prefect flow**:
   ```bash
   uv run src/runner.py
   ```

6. **Start the FastAPI server**:
   ```bash
   fastapi dev api/app.py
   ```