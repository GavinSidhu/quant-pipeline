# Stock Market Data Pipeline

A locally hosted data engineering project for ingesting, processing, and analyzing 
minute-by-minute stock market data from Alpaca API.

## Components

* Data Ingestion: Alpaca API, dlthub
* Processing: Apache Spark
* Storage: Parquet, DuckDB
* Transformation: dbt
* Orchestration: Dagster
* Visualization: Streamlit

## Setup

1. Clone this repository
2. Create a virtual environment: `python -m venv venv`
3. Activate the environment: `source venv/bin/activate` (or `venv\Scripts\activate` on Windows)
4. Install dependencies: `pip install -r requirements.txt`
5. Copy `.env.example` to `.env` and add your Alpaca API keys

## Usage

[To be added as project develops]
