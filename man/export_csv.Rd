% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils.R
\name{export_csv}
\alias{export_csv}
\title{Write data to CSV file}
\usage{
export_csv(data, file, row.names = FALSE)
}
\arguments{
\item{data}{Data frame to write}

\item{file}{Path to output file}

\item{row.names}{Whether to include row names (default: FALSE)}
}
\value{
Invisibly returns the data that was written
}
\description{
Writes data to a CSV file using utils::write.csv
}
\examples{
\dontrun{
# Get data
data <- get_query("SELECT * FROM sensor_data LIMIT 1000")

# Save to CSV
export_csv(data, "sensor_data_export.csv")
}
}
