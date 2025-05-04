# DataHack 2025 - GitHub Archives Analytics

This project implements a medallion architecture for processing and analyzing GitHub Archives data using dbt.

## Architecture

The project follows a medallion architecture with three layers:

1. **Bronze Layer** (Raw Data)
   - Raw GitHub Archives events from S3
   - No transformations applied
   - Maintains data in its original form

2. **Silver Layer** (Cleaned and Validated)
   - Staging models for initial data cleaning
   - Intermediate models for business logic
   - Data quality checks and validations

3. **Gold Layer** (Business Metrics)
   - Marts for business-specific aggregations
   - Metrics for key performance indicators
   - Optimized for query performance

## Setup Instructions

1. **Environment Variables**
   ```bash
   export DBT_DATABASE=your_database
   export DBT_SCHEMA=raw_github
   ```

2. **Installation**
   ```bash
   pip install dbt-snowflake  # or your specific adapter
   ```

3. **Running the Project**
   ```bash
   dbt deps
   dbt run
   dbt test
   ```

## Data Quality

The project includes:
- Automated data quality tests
- Data freshness monitoring
- Schema validation
- Business rule validation

## Performance Optimization

- Materialized views for frequently accessed data
- Partitioned tables for large datasets
- Optimized indexes on key columns

## Testing Strategy

- Unit tests for individual models
- Data quality tests
- Business rule validation
- Relationship integrity tests

## Monitoring

- Data freshness checks
- Error logging
- Performance metrics
- Data quality scores