# Load required packages
library(testthat)
library(DBI)
library(data.table)

# Try to load RPostgres, skip all tests if not available
rpostgres_available <- tryCatch({
  library(RPostgres)
  TRUE
}, error = function(e) {
  FALSE
})

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
  skip_if_not(rpostgres_available, "RPostgres not available")
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

# Test query functions
test_that("query functions work correctly", {
  skip_if_not(rpostgres_available, "RPostgres not available")
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

  # Initialize global connection
  TimescaleR::init_connection(
    host = test_config$host,
    port = test_config$port,
    db = test_config$dbname,
    user = test_config$user,
    pass = test_config$password,
    schema = test_config$schema
  )

  # Create test data
  test_data <- data.table(
    time = seq(as.POSIXct("2020-01-01"), by = "1 hour", length.out = 100),
    value = rnorm(100),
    group = rep(c("A", "B"), length.out = 100)
  )

  # Create test table
  TimescaleR::create_table_from_data_table(
    data_table = test_data,
    table_name = "test_query_table",
    time_column = "time"
  )

  # Test query functions
  result <- TimescaleR::time_bucket(
    table_name = "test_query_table",
    time_column = "time",
    bucket_width = "1 day",
    aggregates = "avg(value) as avg_value, count(*) as count",
    group_by = "group"
  )
  expect_true(is.data.frame(result))

  last_val <- TimescaleR::last_value(
    table_name = "test_query_table",
    value_column = "value",
    time_column = "time"
  )
  expect_true(is.data.frame(last_val))

  first_val <- TimescaleR::first_value(
    table_name = "test_query_table",
    value_column = "value",
    time_column = "time"
  )
  expect_true(is.data.frame(first_val))

  # Cleanup
  TimescaleR::drop_table(table_name = "test_query_table")
  TimescaleR::close_connection()
})

# Test error handling
test_that("query functions handle errors correctly", {
  skip_if_not(rpostgres_available, "RPostgres not available")
  # Test without connection
  if (TimescaleR::has_connection()) {
    TimescaleR::close_connection()
  }

  expect_error(
    TimescaleR::time_bucket(table_name = "test_table", time_column = "time", bucket_width = "1 day"),
    "No global connection initialized"
  )
  expect_error(
    TimescaleR::last_value(table_name = "test_table", value_column = "value", time_column = "time"),
    "No global connection initialized"
  )
})
