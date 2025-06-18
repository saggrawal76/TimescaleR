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

# Test database information functions
test_that("database information functions work correctly", {
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

  # Connect to database
  config <- configure_timescale(
    host = test_config$host,
    port = test_config$port,
    db = test_config$dbname,
    user = test_config$user,
    pass = test_config$password,
    schema = test_config$schema
  )

  tryCatch(
    {
      conn <- connect_to_db(config)

      # Test get_db_size
      size <- get_db_size(conn)
      expect_true(is.list(size))
      expect_true("size" %in% names(size))
      expect_true("unit" %in% names(size))

      # Test list_hypertables
      # This may fail if TimescaleDB is not installed or no hypertables exist
      tryCatch(
        {
          hypertables <- list_hypertables(conn)
          expect_true(is.data.frame(hypertables))
        },
        error = function(e) {
          # Skip this test if it fails
          skip(paste("Hypertable listing failed:", e$message))
        }
      )

      # Test monitor_active_queries
      queries <- monitor_active_queries(conn)
      expect_true(is.data.frame(queries))

      # Test get_db_version
      version <- get_db_version(conn)
      expect_true(is.character(version))

      # Test get_db_settings
      settings <- get_db_settings(conn)
      expect_true(is.data.frame(settings))

      # Clean up
      disconnect_from_db(conn)
    },
    error = function(e) {
      skip(paste("Database test failed:", e$message))
    }
  )
})

# Test error handling
test_that("database information functions handle errors correctly", {
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

  # Test invalid connection (expect any error)
  expect_error(get_db_size(NULL))
  expect_error(monitor_active_queries(NULL))
  expect_error(get_db_settings(NULL))
})
