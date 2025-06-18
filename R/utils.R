# Utility functions that use the imported packages

#' @importFrom magrittr %>%
#' @importFrom rlang :=
#' @importFrom rlang .data
#' @importFrom rlang sym
NULL

#' Plot time series data
#'
#' Creates a time series plot using ggplot2 from data retrieved from TimescaleDB.
#'
#' @param data A data frame containing time series data
#' @param time_col The column containing timestamp data
#' @param value_col The column containing values to plot
#' @param title Plot title (optional)
#' @param color Line color (optional)
#' @param show_points Whether to show points (default: FALSE)
#' @return A ggplot2 plot object
#' @export
#' @examples
#' \dontrun{
#' # Get data
#' data <- get_query("SELECT time, temperature FROM sensor_data LIMIT 100")
#' 
#' # Plot time series
#' plot <- plot_timeseries(data, "time", "temperature", "Sensor Temperature")
#' print(plot)
#' }
plot_timeseries <- function(data, time_col, value_col, title = NULL, 
                           color = "blue", show_points = FALSE) {
  # Validate inputs
  if (!is.data.frame(data)) {
    stop("Data must be a data frame")
  }
  
  if (!time_col %in% colnames(data)) {
    stop(paste("Time column", time_col, "not found in data"))
  }
  
  if (!value_col %in% colnames(data)) {
    stop(paste("Value column", value_col, "not found in data"))
  }
  
  # Create plot using tidy evaluation
  p <- ggplot2::ggplot(data, ggplot2::aes(x = .data[[time_col]], y = .data[[value_col]])) +
    ggplot2::geom_line(color = color) +
    ggplot2::theme_minimal() +
    ggplot2::labs(
      title = title,
      x = "Time",
      y = value_col
    )
  
  # Add points if requested
  if (show_points) {
    p <- p + ggplot2::geom_point(color = color, alpha = 0.5)
  }
  
  return(p)
}

#' Convert JSON data to R objects
#'
#' Utility function to convert JSON data from TimescaleDB to R objects.
#'
#' @param json_string A JSON string
#' @return Parsed R object
#' @export
#' @examples
#' \dontrun{
#' # Get JSON data from database
#' result <- get_query("SELECT data FROM json_data LIMIT 1")
#' json_str <- result$data[1]
#' 
#' # Parse JSON
#' parsed_data <- parse_json(json_str)
#' str(parsed_data)
#' }
parse_json <- function(json_string) {
  jsonlite::fromJSON(json_string)
}

#' Clean and transform data with dplyr and tidyr
#'
#' Cleans and transforms time series data using dplyr and tidyr functions.
#'
#' @param data A data frame containing time series data
#' @param time_col The column containing timestamp data
#' @param value_cols Vector of value column names to process
#' @param pivot Whether to pivot the data to long format (default: FALSE)
#' @return Processed data frame
#' @export
#' @examples
#' \dontrun{
#' # Get data from database
#' data <- get_query("SELECT time, temp, humidity, pressure FROM sensor_data LIMIT 100")
#' 
#' # Clean and transform data
#' clean_data <- clean_timeseries(
#'   data,
#'   time_col = "time",
#'   value_cols = c("temp", "humidity", "pressure"),
#'   pivot = TRUE
#' )
#' }
clean_timeseries <- function(data, time_col, value_cols, pivot = FALSE) {
  # Ensure time column is properly formatted
  data <- data %>%
    dplyr::mutate(
      !!time_col := lubridate::as_datetime(!!rlang::sym(time_col))
    ) %>%
    dplyr::arrange(!!rlang::sym(time_col))
  
  # Remove rows with missing values in specified columns
  data <- data %>%
    dplyr::filter(
      rowSums(is.na(dplyr::select(data, dplyr::all_of(value_cols)))) == 0
    )
  
  # Pivot data if requested
  if (pivot) {
    data <- data %>%
      tidyr::pivot_longer(
        cols = dplyr::all_of(value_cols),
        names_to = "variable",
        values_to = "value"
      )
  }
  
  return(data)
}

#' Extract patterns using stringr
#'
#' Extracts patterns from strings using stringr functions.
#'
#' @param text Character vector to extract from
#' @param pattern Regular expression pattern to extract
#' @return Extracted matches
#' @export
#' @examples
#' \dontrun{
#' # Get data with text fields
#' data <- get_query("SELECT message FROM log_entries LIMIT 100")
#' 
#' # Extract IP addresses
#' ips <- extract_pattern(data$message, "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}")
#' }
extract_pattern <- function(text, pattern) {
  stringr::str_extract_all(text, pattern)
}

#' Write data to CSV file
#'
#' Writes data to a CSV file using utils::write.csv
#'
#' @param data Data frame to write
#' @param file Path to output file
#' @param row.names Whether to include row names (default: FALSE)
#' @return Invisibly returns the data that was written
#' @export
#' @examples
#' \dontrun{
#' # Get data
#' data <- get_query("SELECT * FROM sensor_data LIMIT 1000")
#' 
#' # Save to CSV
#' export_csv(data, "sensor_data_export.csv")
#' }
export_csv <- function(data, file, row.names = FALSE) {
  utils::write.csv(data, file = file, row.names = row.names)
  invisible(data)
}

#' Read data from CSV file
#'
#' Reads data from a CSV file using utils::read.csv
#'
#' @param file Path to CSV file
#' @param stringsAsFactors Whether to convert strings to factors (default: FALSE)
#' @return Data frame
#' @export
#' @examples
#' \dontrun{
#' # Read data from CSV
#' data <- import_csv("sensor_data.csv")
#' 
#' # View structure
#' str(data)
#' }
import_csv <- function(file, stringsAsFactors = FALSE) {
  utils::read.csv(file, stringsAsFactors = stringsAsFactors)
} 