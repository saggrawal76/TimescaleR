# TimescaleR

[![CRAN Status](https://www.r-pkg.org/badges/version/TimescaleR)](https://cran.r-project.org/package=TimescaleR)
[![R-CMD-check](https://github.com/saggrawal76/TimescaleR/workflows/R-CMD-check/badge.svg)](https://github.com/saggrawal76/TimescaleR/actions)

TimescaleR provides an interface to TimescaleDB, a time-series database built on PostgreSQL. The package enables R users to efficiently store, query, analyze, and visualize time-series data using TimescaleDB's specialized time-series functions.

## Features

- **Easy Connection Management**: Global and explicit connection handling
- **Hypertable Operations**: Create and manage TimescaleDB hypertables
- **Time-Series Functions**: Time bucketing, aggregation, and specialized queries
- **Data Management**: Efficient data insertion, backup, and restoration
- **Continuous Aggregates**: Create and manage materialized views
- **Compression & Retention**: Automated data lifecycle management
- **Visualization**: Built-in plotting functions for time-series data

## Installation

You can install TimescaleR from CRAN:

```r
install.packages("TimescaleR")
```

Or install the development version from GitHub:

```r
# install.packages("devtools")
devtools::install_github("saggrawal76/TimescaleR")
```

## Prerequisites

- **TimescaleDB**: You need a running TimescaleDB instance. See [TimescaleDB installation guide](https://docs.timescale.com/install/latest/)
- **PostgreSQL**: TimescaleDB is built on PostgreSQL
- **R packages**: DBI, RPostgres (automatically installed as dependencies)

## Quick Start

```r
library(TimescaleR)

# Initialize a global connection
init_connection(
  host = "localhost",
  port = 5432,
  db = "your_database",
  user = "your_username",
  pass = "your_password",
  schema = "public"
)

# Create sample data
data <- data.frame(
  time = seq(as.POSIXct("2023-01-01"), by = "1 hour", length.out = 100),
  temperature = rnorm(100, 20, 5),
  humidity = rnorm(100, 60, 10)
)

# Create a hypertable
create_table_from_data_table(
  data, 
  "sensor_data", 
  as_hypertable = TRUE, 
  time_column = "time"
)

# Query with time bucketing
hourly_avg <- time_bucket(
  table_name = "sensor_data",
  time_column = "time",
  bucket_interval = "1 hour",
  value_column = "temperature"
)

# Close connection when done
close_connection()
```

## Core Functions

### Connection Management
- `init_connection()`: Initialize global database connection
- `get_connection()`: Get the current connection
- `close_connection()`: Close the global connection
- `has_connection()`: Check if a connection exists

### Data Operations
- `create_table_from_data_table()`: Create tables from R data frames
- `convert_to_hypertable()`: Convert regular tables to hypertables
- `insert_data_to_table()`: Insert data with batching support
- `get_query()`: Execute SQL queries

### Time-Series Analysis
- `time_bucket()`: Aggregate data into time intervals
- `first_value()` / `last_value()`: Get first/last values in time series
- `moving_average()`: Calculate moving averages
- `interpolate()`: Interpolate missing time-series data

### Advanced Features
- `create_continuous_aggregate()`: Create materialized views
- `add_compression_policy()`: Automatic data compression
- `add_retention_policy()`: Automatic data retention
- `backup_hypertable()` / `restore_hypertable()`: Data backup/restore

## Examples

### Working with Time-Series Data

```r
# Time bucketing with multiple metrics
result <- time_bucket(
  table_name = "sensor_data",
  time_column = "timestamp",
  bucket_interval = "1 day",
  value_column = c("temperature", "humidity"),
  group_by = "sensor_id"
)

# Moving average calculation
ma_result <- moving_average(
  table_name = "sensor_data",
  value_column = "temperature",
  time_column = "timestamp",
  window_size = 7
)
```

### Continuous Aggregates

```r
# Create a continuous aggregate for daily averages
create_continuous_aggregate(
  view_name = "daily_weather",
  table_name = "weather_data",
  time_column = "timestamp",
  interval = "1 day",
  select_expr = "time_bucket('1 day', timestamp) AS day,
                 AVG(temperature) AS avg_temp,
                 MAX(temperature) AS max_temp,
                 MIN(temperature) AS min_temp"
)

# Add automatic refresh policy
add_refresh_policy("daily_weather", "1 hour")
```

### Data Lifecycle Management

```r
# Add compression policy (compress data older than 7 days)
add_compression_policy("sensor_data", "7 days")

# Add retention policy (drop data older than 1 year)
add_retention_policy("sensor_data", "1 year")
```

## Documentation

For detailed documentation, see:
- Package documentation: `help(package = "TimescaleR")`
- Function help: `?function_name`
- [TimescaleDB documentation](https://docs.timescale.com/)

## Contributing

Contributions are welcome! Please see our [contributing guidelines](CONTRIBUTING.md) and feel free to submit issues and pull requests.

## License

This package is licensed under the MIT License. See [LICENSE](LICENSE) file for details.

## Citation

To cite TimescaleR in publications, use:

```r
citation("TimescaleR")
```

## Support

- **Issues**: [GitHub Issues](https://github.com/saggrawal76/TimescaleR/issues)
- **Documentation**: Package help files and vignettes
- **TimescaleDB Community**: [TimescaleDB Slack](https://timescaledb.slack.com/) 