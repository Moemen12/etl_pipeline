# ETL Pipeline for Caregiver Management System

A robust ETL pipeline designed to process and store caregiver and carelog data from CSV files into a PostgreSQL database. This solution handles complex healthcare data with proper data validation, error handling, and performance optimization.

## Schema Design Rationale

### Database Structure
The schema is designed around two core entities with specific considerations for healthcare data management:

#### Caregivers Table
- **Primary Key**: `profile_id` - Ensures unique caregiver identification across the system
- **Unique Constraint**: `caregiver_id` - Provides alternative identification method
- **Multi-tenant Support**: `franchisor_id` and `agency_id` enable data isolation between different healthcare organizations
- **Flexible Location Tracking**: `locations_id` (INT) and `location_name` (VARCHAR) support both structured and unstructured location data
- **Status Tracking**: `applicant_status` and `status` fields handle complex caregiver lifecycle states

#### Carelogs Table
- **Primary Key**: `carelog_id` - Unique identifier for each care session
- **Foreign Key**: `caregiver_id` references caregivers table for referential integrity
- **Time Tracking**: Separate fields for scheduled (`start_datetime`, `end_datetime`) and actual (`clock_in_actual_datetime`, `clock_out_actual_datetime`) times enable overtime analysis
- **Audit Trail**: `clock_in_method` and `clock_out_method` track how time entries were recorded
- **Documentation**: `documentation` and `general_comment_char_count` support compliance requirements

### Design Decisions

**Why separate franchisor_id and agency_id?**
- Enables hierarchical data access (franchisor can view all agencies, agencies see only their data)
- Supports complex healthcare organizational structures
- Allows for future multi-tenant features

**Why track both scheduled and actual times?**
- Enables overtime calculation: `actual_duration - scheduled_duration`
- Supports reliability analysis: comparing scheduled vs actual clock-in times
- Provides audit trail for compliance and billing purposes

**Why use nullable fields extensively?**
- Healthcare data is often incomplete or optional
- Prevents data loss from partial records
- Enables gradual data enrichment over time

## Assumptions & Edge Cases

### Data Quality Assumptions

1. **Timestamp Handling**
   - Assumes ISO 8601 format for datetime strings
   - Invalid dates are converted to `null` rather than causing pipeline failure
   - Empty strings and 'None' values are treated as `null`

2. **Boolean Field Processing**
   - String values 'True'/'False' are converted to boolean
   - Invalid boolean values become `null` to preserve data integrity
   - Handles case variations gracefully

3. **Numeric Field Validation**
   - String numbers are parsed to integers
   - '0' values for IDs are treated as `null` (assuming they represent missing data)
   - Invalid numbers become `null` rather than causing errors

### Edge Case Handling

**Corrupted Data**
- Invalid rows are silently skipped with error logging
- Pipeline continues processing valid records
- No single bad record can halt the entire process

**Missing Required Fields**
- Carelogs without valid `start_datetime` or `end_datetime` are filtered out
- Caregivers with missing `profile_id` or `caregiver_id` are skipped
- Foreign key violations are prevented through data validation

**Data Type Inconsistencies**
- String numbers are automatically converted to appropriate types
- Date parsing handles multiple formats and invalid dates
- Boolean strings are normalized to true/false/null

**Duplicate Handling**
- Uses `ON CONFLICT DO NOTHING` to handle duplicate primary keys
- Prevents data corruption from repeated pipeline runs
- Maintains idempotency for the ETL process

## Scalability & Performance

### Current Performance Optimizations

**Batch Processing**
- Processes 1,000 records per database transaction
- Reduces database round trips by 99% compared to individual inserts
- Balances memory usage with transaction size

**Connection Pooling**
- PostgreSQL connection pool with max 20 connections
- Prevents connection exhaustion under load
- Enables concurrent processing capabilities

**Memory Management**
- Streams CSV data rather than loading entire files into memory
- Processes data in chunks to handle large datasets
- Releases database connections promptly

### Scalability Considerations

**For Larger Datasets (10M+ records)**

**Database Optimizations**
```sql
-- Recommended indexes for performance
CREATE INDEX idx_caregivers_agency_id ON caregivers(agency_id);
CREATE INDEX idx_caregivers_caregiver_id ON caregivers(caregiver_id);
CREATE INDEX idx_carelogs_caregiver_id ON carelogs(caregiver_id);
CREATE INDEX idx_carelogs_datetime ON carelogs(start_datetime, end_datetime);
CREATE INDEX idx_carelogs_agency_id ON carelogs(agency_id);
```

**Pipeline Enhancements**
- Implement parallel processing for independent data sources
- Add data partitioning by agency_id for parallel loads
- Implement checkpoint/resume functionality for long-running jobs
- Add progress monitoring and estimated completion times

**Infrastructure Scaling**
- Horizontal scaling: Multiple ETL instances processing different agencies
- Database read replicas for analytics queries
- Separate staging and production databases
- Implement data archival strategies for historical data

### Performance Trade-offs

**Memory vs Speed**
- Current approach prioritizes memory efficiency over raw speed
- Could increase batch size to 5,000-10,000 for faster processing
- Trade-off: Higher memory usage vs reduced database calls

**Data Validation vs Performance**
- Comprehensive validation ensures data quality but adds processing time
- Could implement staged validation (basic during load, full during off-peak)
- Trade-off: Data integrity vs processing speed

**Error Handling vs Performance**
- Silent error handling prevents pipeline failures but may hide data issues
- Could implement error aggregation and reporting
- Trade-off: Pipeline reliability vs data quality visibility

### Monitoring & Observability

The pipeline includes performance metrics:
- Records processed per second
- Individual step timing
- Success/failure rates
- Memory usage tracking

For production deployment, consider adding:
- Real-time monitoring dashboards
- Alerting for pipeline failures
- Data quality metrics
- Performance trend analysis

## Getting Started

1. Install dependencies: `npm install`
2. Configure environment variables in `.env`
3. Run the pipeline: `npm run start`

The pipeline will process CSV files from the `data/` directory and load them into PostgreSQL with comprehensive error handling and performance optimization.

**Don't forget to put `caregivers.csv` and `carelogs.csv` files in the `data/` folder**

## Sample Output

Here's what you can expect when running the ETL pipeline:

```bash
moemen@moemen-IdeaPad-Slim-3-15IRH8:~/Desktop/etl-assessment$ npm run start

> etl-assessment@1.0.0 start
> npx tsx src/index.ts

[dotenv@17.2.1] injecting env (5) from .env -- tip: ⚙️  suppress all logs with { quiet: true }
🚀 Starting ETL process...
Finished reading /home/moemen/Desktop/etl-assessment/data/caregivers.csv. Total rows: 1004888
📁 Extract Caregivers: 1,004,888 rows in 9.63s
Finished reading /home/moemen/Desktop/etl-assessment/data/carelogs.csv. Total rows: 308602
📁 Extract Carelogs: 308,602 rows in 5.15s
Transformed 1004888 caregivers out of 1004888 rows
🔄 Transform Caregivers: 1,004,888 records in 0.33s
Transformed 303244 carelogs out of 308602 rows
🔄 Transform Carelogs: 303,244 records in 0.46s
💾 Database Init: 0.02s
💾 Insert Caregivers: 1,004,888 records in 32.80s
💾 Insert Carelogs: 303,244 records in 9.42s
🎉 ETL Completed: 1,308,132 total records in 57.81s
📊 Performance: 22,628 records/second
PostgreSQL connection pool closed.
```

### Performance Summary

- **Total Records Processed**: 1,308,132
- **Processing Time**: 57.81 seconds
- **Throughput**: 22,628 records/second
- **Data Quality**: 5,358 carelog records were filtered out due to invalid datetime fields
- **Success Rate**: 99.6% (1,004,888/1,004,888 caregivers, 303,244/308,602 carelogs)
