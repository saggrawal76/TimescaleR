library(testthat)
library(TimescaleR)

# Mock tests that don't require database connection
test_that("configure_timescale validates input parameters", {
  # Test valid configuration
  config <- configure_timescale(
    host = "localhost",
    port = 5432,
    db = "testdb",
    user = "testuser",
    pass = "testpass",
    schema = "public"
  )
  
  expect_type(config, "list")
  expect_equal(config$host, "localhost")
  expect_equal(config$port, 5432)
  expect_equal(config$db, "testdb")
  expect_equal(config$user, "testuser")
  expect_equal(config$schema, "public")
})

test_that("configure_timescale handles invalid inputs", {
  # Test invalid host
  expect_error(
    configure_timescale(
      host = "",
      port = 5432,
      db = "testdb",
      user = "testuser",
      pass = "testpass"
    ),
    "Invalid host"
  )
  
  # Test invalid port
  expect_error(
    configure_timescale(
      host = "localhost",
      port = -1,
      db = "testdb",
      user = "testuser",
      pass = "testpass"
    ),
    "Invalid port"
  )
  
  expect_error(
    configure_timescale(
      host = "localhost",
      port = 70000,
      db = "testdb",
      user = "testuser",
      pass = "testpass"
    ),
    "Invalid port"
  )
  
  # Test invalid database name
  expect_error(
    configure_timescale(
      host = "localhost",
      port = 5432,
      db = "",
      user = "testuser",
      pass = "testpass"
    ),
    "Invalid database name"
  )
  
  # Test invalid user
  expect_error(
    configure_timescale(
      host = "localhost",
      port = 5432,
      db = "testdb",
      user = "",
      pass = "testpass"
    ),
    "Invalid user"
  )
  
  # Test invalid password
  expect_error(
    configure_timescale(
      host = "localhost",
      port = 5432,
      db = "testdb",
      user = "testuser",
      pass = 123
    ),
    "Invalid password"
  )
  
  # Test invalid schema
  expect_error(
    configure_timescale(
      host = "localhost",
      port = 5432,
      db = "testdb",
      user = "testuser",
      pass = "testpass",
      schema = ""
    ),
    "Invalid schema"
  )
})

test_that("package environment functions work", {
  # Test has_connection when no connection exists
  expect_false(has_connection(silent = TRUE))
  
  # Test debug_environment returns proper structure
  debug_info <- debug_environment(verbose = FALSE)
  expect_type(debug_info, "list")
  expect_true("timestamp" %in% names(debug_info))
  expect_true("r_version" %in% names(debug_info))
  expect_true("package_version" %in% names(debug_info))
})

# Test utility functions that don't require database
test_that("utility functions work correctly", {
  # Test parse_json
  json_str <- '{"key": "value", "number": 42}'
  result <- parse_json(json_str)
  expect_equal(result$key, "value")
  expect_equal(result$number, 42)
  
  # Test extract_pattern
  text <- c("IP: 192.168.1.1", "IP: 10.0.0.1", "No IP here")
  pattern <- "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}"
  result <- extract_pattern(text, pattern)
  expect_length(result, 3)
  expect_equal(result[[1]], "192.168.1.1")
  expect_equal(result[[2]], "10.0.0.1")
  expect_length(result[[3]], 0)
})

test_that("data validation functions work", {
  # Test adapt_data_types
  data <- data.frame(
    timestamp = as.POSIXct("2023-01-01 12:00:00"),
    date = as.Date("2023-01-01"),
    value = 42.5,
    flag = TRUE,
    text = "test",
    stringsAsFactors = FALSE
  )
  
  adapted <- adapt_data_types(data)
  expect_type(adapted$timestamp, "character")
  expect_type(adapted$date, "character")
  expect_type(adapted$value, "double")
  expect_type(adapted$flag, "logical")
  expect_type(adapted$text, "character")
  
  # Test clean_data_for_primary_keys
  data_with_na <- data.frame(
    id = c(1, 2, NA, 4, 5),
    value = c(10, 20, 30, 40, 50)
  )
  
  cleaned <- clean_data_for_primary_keys(data_with_na, "id")
  expect_equal(nrow(cleaned), 4)
  expect_false(any(is.na(cleaned$id)))
  
  # Test determine_column_types
  test_data <- data.frame(
    timestamp = as.POSIXct("2023-01-01 12:00:00"),
    date = as.Date("2023-01-01"),
    number = 123.45,
    flag = TRUE,
    text = "example"
  )
  
  col_types <- determine_column_types(test_data)
  expect_equal(col_types[["timestamp"]], "TIMESTAMP")
  expect_equal(col_types[["date"]], "DATE")
  expect_equal(col_types[["number"]], "DOUBLE PRECISION")
  expect_equal(col_types[["flag"]], "BOOLEAN")
  expect_equal(col_types[["text"]], "TEXT")
})

# Test CSV functions
test_that("CSV functions work correctly", {
  # Create temporary file
  temp_file <- tempfile(fileext = ".csv")
  
  # Test data
  test_data <- data.frame(
    time = as.POSIXct(c("2023-01-01 12:00:00", "2023-01-01 13:00:00")),
    value = c(10.5, 20.3),
    category = c("A", "B")
  )
  
  # Test export_csv
  result <- export_csv(test_data, temp_file)
  expect_true(file.exists(temp_file))
  expect_identical(result, test_data)
  
  # Test import_csv
  imported_data <- import_csv(temp_file)
  expect_equal(nrow(imported_data), 2)
  expect_equal(ncol(imported_data), 3)
  
  # Clean up
  unlink(temp_file)
})

test_that("plot_timeseries validates inputs correctly", {
  # Test with valid data
  data <- data.frame(
    time = as.POSIXct(c("2023-01-01 12:00:00", "2023-01-01 13:00:00")),
    value = c(10.5, 20.3)
  )
  
  plot <- plot_timeseries(data, "time", "value", "Test Plot")
  expect_s3_class(plot, "ggplot")
  
  # Test error handling
  expect_error(
    plot_timeseries("not a data frame", "time", "value"),
    "Data must be a data frame"
  )
  
  expect_error(
    plot_timeseries(data, "nonexistent", "value"),
    "Time column nonexistent not found in data"
  )
  
  expect_error(
    plot_timeseries(data, "time", "nonexistent"),
    "Value column nonexistent not found in data"
  )
})
