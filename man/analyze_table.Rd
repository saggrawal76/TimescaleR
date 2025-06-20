% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/tables.R
\name{analyze_table}
\alias{analyze_table}
\title{Analyze a Table}
\usage{
analyze_table(
  table_name,
  verbose = FALSE,
  auto_reconnect = TRUE,
  debug = FALSE
)
}
\arguments{
\item{table_name}{Name of the table to analyze}

\item{verbose}{Logical, whether to print detailed information (default: FALSE)}

\item{auto_reconnect}{Whether to attempt reconnection if the connection is lost (default: TRUE)}

\item{debug}{Enable verbose debug output (default: FALSE)}
}
\value{
TRUE if successful
}
\description{
Analyzes a table to update statistics used by the query planner.
ANALYZE is a PostgreSQL operation that collects statistics about the contents
of tables in the database, which helps the query planner generate better execution plans.
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

# Analyze a table
analyze_table("my_table")

# Analyze with verbose output
analyze_table("my_table", verbose = TRUE)
}
}
