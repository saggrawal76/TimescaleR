# TimescaleR 0.1.3

## New Features

* Initial CRAN release
* Complete interface to TimescaleDB for R users
* Global connection management with `init_connection()` and `close_connection()`
* Hypertable creation and management functions
* Time-series specific query functions including `time_bucket()`, `first_value()`, `last_value()`
* Continuous aggregates support with automated refresh policies
* Data compression and retention policy management
* Backup and restore functionality for hypertables
* Built-in visualization functions for time-series data
* Comprehensive error handling and debugging tools

## Core Functions

### Connection Management
* `init_connection()`: Initialize global database connection
* `get_connection()`: Retrieve current connection
* `close_connection()`: Close global connection
* `has_connection()`: Check connection status
* `debug_environment()`: Debug package environment state

### Data Operations
* `create_table_from_data_table()`: Create tables from R data frames
* `convert_to_hypertable()`: Convert regular tables to hypertables
* `insert_data_to_table()`: Batch data insertion
* `append_data_table_to_table()`: Append data to existing tables
* `get_query()`: Execute SQL queries with parameter binding

### Time-Series Analysis
* `time_bucket()`: Aggregate data into time intervals
* `first_value()` / `last_value()`: Get boundary values in time series
* `moving_average()`: Calculate moving averages with window functions
* `time_weighted_average()`: Calculate time-weighted averages
* `interpolate()`: Linear interpolation for missing time-series data

### Advanced Features
* `create_continuous_aggregate()`: Create materialized views
* `refresh_continuous_aggregate()`: Manual refresh of continuous aggregates
* `add_compression_policy()`: Automatic data compression
* `add_retention_policy()`: Automatic data retention
* `backup_hypertable()` / `restore_hypertable()`: Data backup and restore

### Utility Functions
* `plot_timeseries()`: Built-in time-series plotting
* `clean_timeseries()`: Data cleaning and transformation
* `export_csv()` / `import_csv()`: CSV import/export functionality
* `parse_json()`: JSON data parsing
* `extract_pattern()`: Regular expression pattern extraction

## Documentation

* Comprehensive documentation for all functions
* Detailed examples in help files
* Package vignettes for common use cases
* README with quick start guide

## Testing

* Extensive test suite with >90% code coverage
* Mock tests that don't require external database connections
* Input validation tests for all major functions
* Error handling verification

## Dependencies

* R (>= 3.5.0)
* DBI (>= 1.0.0)
* RPostgres (>= 1.3.0)
* data.table (>= 1.12.0)
* dplyr (>= 1.0.0)
* ggplot2 (>= 3.3.0)
* Other tidyverse packages for data manipulation and visualization 