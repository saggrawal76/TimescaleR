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

# Test continuous aggregate operations
test_that("continuous aggregate operations work correctly", {
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

  # Create test data
  test_data <- data.table(
    time = seq(as.POSIXct("2020-01-01"), by = "1 hour", length.out = 100),
    value = rnorm(100)
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

  # Create test table
  TimescaleR::create_table_from_data_table(
    data_table = test_data,
    table_name = "test_table",
    time_column = "time"
  )

  # Create continuous aggregate
  TimescaleR::create_continuous_aggregate(
    view_name = "test_agg",
    query = paste0(
      "SELECT time_bucket('1 day', time) as bucket, ",
      "avg(value) as avg_value FROM test_table GROUP BY bucket"
    )
  )

  # Refresh continuous aggregate
  TimescaleR::refresh_continuous_aggregate(view_name = "test_agg")

  # Add refresh policy
  TimescaleR::add_refresh_policy(
    view_name = "test_agg",
    refresh_interval = "1 hour"
  )

  # List continuous aggregates
  aggs <- TimescaleR::list_continuous_aggregates()
  expect_true("test_agg" %in% aggs$view_name)

  # Drop continuous aggregate
  TimescaleR::drop_continuous_aggregate(view_name = "test_agg")

  # Clean up
  TimescaleR::drop_table(table_name = "test_table")
  TimescaleR::close_connection()
})

# Test error handling
test_that("continuous aggregate functions handle errors correctly", {
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

  # Test invalid table
  expect_error(
    TimescaleR::create_continuous_aggregate(
      view_name = "test_agg",
      query = paste0(
        "SELECT time_bucket('1 day', time) as bucket, ",
        "avg(value) as avg_value FROM non_existent_table GROUP BY bucket"
      )
    ),
    "Failed to create continuous aggregate"
  )

  # Test invalid time column
  expect_error(
    TimescaleR::create_continuous_aggregate(
      view_name = "test_agg",
      query = paste0(
        "SELECT time_bucket('1 day', invalid_column) as bucket, ",
        "avg(value) as avg_value FROM test_table GROUP BY bucket"
      )
    ),
    "Failed to create continuous aggregate"
  )

  # Clean up
  TimescaleR::close_connection()
})
