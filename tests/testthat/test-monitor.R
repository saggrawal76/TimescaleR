# Load required packages
library(testthat)
library(DBI)

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

# Test monitoring functions
test_that("monitoring functions work correctly", {
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

  # Test monitoring functions with global connection
  queries <- TimescaleR::monitor_active_queries()
  expect_type(queries, "list")

  locks <- TimescaleR::monitor_locks()
  expect_type(locks, "list")

  hypertables <- TimescaleR::list_hypertables()
  expect_type(hypertables, "list")

  # Clean up
  TimescaleR::close_connection()
})

# Test error handling
test_that("monitoring functions handle errors correctly", {
  skip_if_not(rpostgres_available, "RPostgres not available")
  # Test invalid connection
  if (TimescaleR::has_connection()) {
    TimescaleR::close_connection()
  }
  expect_error(TimescaleR::monitor_active_queries(), "No global connection initialized")
  expect_error(TimescaleR::monitor_locks(NULL), "Invalid database connection")
  expect_error(TimescaleR::list_hypertables(NULL), "Invalid database connection")
})
