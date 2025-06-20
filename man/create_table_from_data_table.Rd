% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/tables.R
\name{create_table_from_data_table}
\alias{create_table_from_data_table}
\title{Create Table from Data Table}
\usage{
create_table_from_data_table(
  data_table,
  table_name,
  conn = NULL,
  primary_keys = NULL,
  date_columns = NULL,
  date_time_columns = NULL,
  as_hypertable = FALSE,
  time_column = NULL,
  schema = NULL,
  chunk_time_interval = "1 day",
  if_not_exists = FALSE,
  index_columns = NULL,
  primary_key = NULL,
  clean_nulls = TRUE,
  null_replacement = "UNKNOWN",
  debug = FALSE
)
}
\arguments{
\item{data_table}{data.table object to create table from}

\item{table_name}{Name of the table to create}

\item{conn}{Database connection object (optional if global connection is initialized)}

\item{primary_keys}{Vector of column names to use as primary keys}

\item{date_columns}{Vector of column names to convert to DATE type}

\item{date_time_columns}{Vector of column names to convert to TIMESTAMP type}

\item{as_hypertable}{Logical, whether to convert to hypertable}

\item{time_column}{Name of the time column (required if as_hypertable is TRUE)}

\item{schema}{Schema name (optional)}

\item{chunk_time_interval}{Interval for chunking (e.g., "1 day")}

\item{if_not_exists}{Logical, whether to skip if table exists}

\item{index_columns}{Columns to index}

\item{primary_key}{Primary key column(s)}

\item{clean_nulls}{Logical, whether to remove or replace NULL values in primary keys (default: TRUE)}

\item{null_replacement}{Value to replace NULLs with in primary key columns if clean_nulls is TRUE (default: "UNKNOWN")}

\item{debug}{Enable debug messages (optional, default FALSE)}
}
\value{
TRUE if successful
}
\description{
Creates a new table in TimescaleDB from a data.table object.
Optionally converts it to a hypertable.
}
\examples{
\dontrun{
# Create a sample data.table
data <- data.table::data.table(
  time = seq(as.POSIXct("2023-01-01"), by = "1 hour", length.out = 24),
  value = rnorm(24),
  category = rep(c("A", "B"), each = 12)
)

# Using global connection with required arguments first
init_connection(config)
create_table_from_data_table(data, "my_table", as_hypertable = TRUE, time_column = "time")
close_connection()

# With explicit connection as optional parameter
conn <- connect_to_db(config)
create_table_from_data_table(data, "my_table", conn, as_hypertable = TRUE, time_column = "time")
disconnect_from_db(conn)
}
}
