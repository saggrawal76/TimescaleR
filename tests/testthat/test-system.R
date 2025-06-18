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

# Test maintenance functions
test_that("maintenance functions work correctly", {
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

  # Test maintenance functions
  expect_true(TimescaleR::run_maintenance())
  expect_true(TimescaleR::optimize_db())

  # Test settings
  settings <- TimescaleR::check_postgres_settings()
  expect_type(settings, "data.frame")

  # Clean up
  TimescaleR::close_connection()
})

# Test backup and restore functions
test_that("backup and restore functions work correctly", {
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
    value = rnorm(100)
  )

  # Create test table
  TimescaleR::create_table_from_data_table(
    data_table = test_data,
    table_name = "test_backup_table",
    time_column = "time"
  )

  # Test backup
  backup_file <- tempfile(fileext = ".csv")
  expect_true(TimescaleR::backup_hypertable(table_name = "test_backup_table", backup_path = backup_file))
  expect_true(file.exists(backup_file))

  # Test restore
  expect_true(TimescaleR::restore_hypertable(table_name = "test_restore_table", backup_path = backup_file))

  # Cleanup
  TimescaleR::drop_table(table_name = "test_backup_table")
  TimescaleR::drop_table(table_name = "test_restore_table")
  unlink(backup_file)

  TimescaleR::close_connection()
})

# Test error handling
test_that("system functions handle errors correctly", {
  skip_if_not(rpostgres_available, "RPostgres not available")
  # Test without connection
  if (TimescaleR::has_connection()) {
    TimescaleR::close_connection()
  }

  expect_error(TimescaleR::run_maintenance(), "No global connection initialized")
  expect_error(TimescaleR::optimize_db(), "No global connection initialized")
  expect_error(TimescaleR::check_postgres_settings(), "No global connection initialized")
})
