% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/config.R
\name{get_search_path}
\alias{get_search_path}
\title{Get search path}
\usage{
get_search_path(conn = NULL, debug = FALSE)
}
\arguments{
\item{conn}{Database connection object (optional, uses global connection if NULL)}

\item{debug}{Enable debug messages (optional, default FALSE)}
}
\value{
Current search path
}
\description{
Get search path
}
\examples{
\dontrun{
# Using global connection
init_connection(dbname = "testdb", host = "localhost")
path <- get_search_path()

# Or with explicit connection
conn <- get_connection()
path <- get_search_path(conn)
}
}
