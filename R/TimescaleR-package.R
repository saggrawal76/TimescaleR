#' TimescaleR: A Package for Working with TimescaleDB in R
#'
#' TimescaleR provides a seamless interface between R and TimescaleDB,
#' making it easy to work with time-series data stored in TimescaleDB.
#' The package offers functionality for query execution, data manipulation,
#' time series analysis, and more.
#'
#' @section Getting Started:
#' To use TimescaleR, you need to first establish a connection to your TimescaleDB database.
#' The package supports two connection methods:
#' \itemize{
#'   \item Global connection (Recommended): Initialize once and use across all functions
#'   \item Explicit connection: Pass a connection object to each function
#' }
#'
#' @section Global Connection Example:
#' \preformatted{
#' # Load the package
#' library(TimescaleR)
#' 
#' # Initialize a global connection
#' init_connection(
#'   host = "localhost",
#'   port = 5432,
#'   db = "testdb",
#'   user = "testuser",
#'   pass = "testpass",
#'   schema = "public"
#' )
#' 
#' # Now you can use other functions without passing a connection
#' result <- get_query("SELECT * FROM my_table LIMIT 10")
#' 
#' # Run a time bucket query to aggregate data
#' hourly_data <- time_bucket(
#'   table_name = "sensor_data",
#'   time_column = "timestamp",
#'   bucket_interval = "1 hour",
#'   value_column = "temperature"
#' )
#' 
#' # Always close the connection when done
#' close_connection()
#' }
#'
#' @section IMPORTANT - Using with Package Namespace:
#' If you're calling functions with the package namespace (e.g., TimescaleR::get_query()),
#' the global connection won't be shared between function calls. In this case, you need to:
#' \preformatted{
#' # Initialize using the full namespace (saves in package environment)
#' conn <- TimescaleR::init_connection(
#'   host = "localhost",
#'   port = 5432,
#'   db = "testdb",
#'   user = "testuser",
#'   pass = "testpass"
#' )
#' 
#' # Use the connection explicitly with each function call
#' result <- TimescaleR::get_query(
#'   "SELECT * FROM my_table LIMIT 10"
#' )
#' 
#' # Or store the connection and pass it explicitly
#' # by setting auto_reconnect to FALSE to avoid unnecessary checks
#' result <- TimescaleR::get_query(
#'   "SELECT * FROM my_table LIMIT 10",
#'   auto_reconnect = FALSE
#' )
#' }
#'
#' @section Working with Time Series Data:
#' TimescaleR provides several specialized functions for time series analysis:
#' \itemize{
#'   \item \code{\link{time_bucket}}: Aggregate data into regular time intervals
#'   \item \code{\link{first_value}}: Get the earliest value in a time series
#'   \item \code{\link{last_value}}: Get the most recent value in a time series
#'   \item \code{\link{moving_average}}: Calculate a moving average over a window
#'   \item \code{\link{time_weighted_average}}: Calculate time-weighted averages
#'   \item \code{\link{interpolate}}: Perform linear interpolation on time series
#' }
#'
#' @section Table Management:
#' Manage TimescaleDB tables with these functions:
#' \itemize{
#'   \item \code{\link{create_table_from_data_table}}: Create a table from a data.table
#'   \item \code{\link{convert_to_hypertable}}: Convert a regular table to a hypertable
#'   \item \code{\link{vacuum_table}}: Clean up a table and reclaim space
#'   \item \code{\link{analyze_table}}: Update statistics for the query planner
#' }
#'
#' @docType package
#' @name TimescaleR
"_PACKAGE"
NULL 
