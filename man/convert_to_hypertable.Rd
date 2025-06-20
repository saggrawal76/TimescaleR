% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/tables.R
\name{convert_to_hypertable}
\alias{convert_to_hypertable}
\title{Convert a regular table to a hypertable}
\usage{
convert_to_hypertable(
  table_name,
  time_column,
  conn = NULL,
  schema = NULL,
  chunk_time_interval = "1 day",
  if_not_exists = FALSE,
  migrate_data = TRUE,
  auto_reconnect = TRUE,
  debug = FALSE
)
}
\arguments{
\item{table_name}{Name of the table to convert}

\item{time_column}{Name of the time column}

\item{conn}{Database connection object (optional if global connection is initialized)}

\item{schema}{Schema name (optional)}

\item{chunk_time_interval}{Interval for chunking (default: "1 day")}

\item{if_not_exists}{Whether to skip if hypertable already exists (default: FALSE)}

\item{migrate_data}{Whether to migrate data to the new hypertable (default: TRUE)}

\item{auto_reconnect}{Whether to attempt reconnection if the connection is lost (default: TRUE)}

\item{debug}{Enable verbose debug output (default: FALSE)}
}
\value{
TRUE if conversion was successful
}
\description{
Converts a regular PostgreSQL table to a TimescaleDB hypertable.
A hypertable is a table that is automatically partitioned by time.
}
\examples{
\dontrun{
# Initialize connection
init_connection(
  host = "localhost",
  port = 5432,
  db = "testdb",
  user = "testuser",
  pass = "testpass"
)

# Convert a table to hypertable
convert_to_hypertable(conn, "my_table", "time_column",
  schema = "public", chunk_time_interval = "1 day"
)
}
}
