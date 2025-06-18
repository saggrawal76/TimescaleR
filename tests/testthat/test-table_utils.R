# Load required packages
library(pacman)
p_load(testthat, DBI, RPostgres, data.table, lubridate)

# Test configuration
test_config <- list(
  host = "localhost",
  port = 5432,
  dbname = "testdb",
  user = "testuser",
  password = "testpass",
  schema = "public"
)

# Test if database is available
test_that("database is available", {
  skip_if_not(
    tryCatch(
      {
        conn <- DBI::dbConnect(
          RPostgres::Postgres(),
          host = test_config$host,
          port = test_config$port,
          dbname = test_config$dbname,
          user = test_config$user,
          password = test_config$password
        )
        DBI::dbDisconnect(conn)
        TRUE
      },
      error = function(e) FALSE
    ),
    "Database not available"
  )
})

# Test table utility functions
test_that("table utility functions work correctly", {
  # Create test data
  test_data <- data.table(
    time = Sys.time(),
    value = 1:5,
    name = letters[1:5],
    flag = c(TRUE, FALSE, TRUE, FALSE, TRUE),
    date = Sys.Date()
  )

  # Test convert_date_time_columns
  date_time_columns <- c("time")
  converted_data <- TimescaleR::convert_date_time_columns(test_data, date_time_columns = date_time_columns)
  expect_true(inherits(converted_data$time, "POSIXct"))
  expect_true(inherits(converted_data$date, "Date"))

  # Test adapt_data_types
  adapted_data <- TimescaleR::adapt_data_types(test_data)
  expect_true(is.character(adapted_data$time))
  expect_true(is.character(adapted_data$date))

  # Test determine_column_types
  types <- TimescaleR::determine_column_types(test_data)
  expect_equal(types[["time"]], "TIMESTAMP")
  expect_equal(types[["value"]], "DOUBLE PRECISION")
  expect_equal(types[["name"]], "TEXT")
  expect_equal(types[["flag"]], "BOOLEAN")
  expect_equal(types[["date"]], "DATE")
})

# Test error handling
test_that("table utility functions handle errors correctly", {
  # Create test data with NA values
  test_data <- data.table(
    time = c(Sys.time(), Sys.time(), NA),
    value = c(1, 2, 3)
  )

  # Test clean_data_for_primary_keys with NA values in primary key
  cleaned_data <- TimescaleR::clean_data_for_primary_keys(test_data, "time")
  expect_equal(nrow(cleaned_data), 2) # Should remove the row with NA

  # Test invalid input
  expect_error(TimescaleR::convert_date_time_columns(NULL, c("time")), "Data cannot be NULL")
})
