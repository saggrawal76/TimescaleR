# Load required packages
library(testthat)
library(data.table)
library(DBI)
library(TimescaleR)

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

test_that("create_table_from_data_table works correctly", {
  skip_if_not(rpostgres_available, "RPostgres not available")
  # Skip if database not available
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
    },
    error = function(e) {
      skip("Database not available for testing")
    }
  )

  # Create test data
  test_data <- data.table::data.table(
    time = seq(as.POSIXct("2024-01-01"), by = "1 hour", length.out = 24),
    value = rnorm(24),
    group = rep(c("A", "B"), each = 12)
  )

  # Create test connection
  config <- configure_timescale(
    host = test_config$host,
    port = test_config$port,
    db = test_config$dbname,
    user = test_config$user,
    pass = test_config$password
  )
  conn <- connect_to_db(config)

  # Test table creation with schema
  expect_silent(
    create_table_from_data_table(
      conn,
      test_data,
      "test_table",
      schema = test_config$schema,
      primary_keys = "time",
      date_time_columns = "time",
      as_hypertable = TRUE,
      time_column = "time",
      chunk_time_interval = "1 day"
    )
  )

  # Verify table exists
  expect_true(table_exists(conn, "test_table"))

  # Test if_not_exists parameter
  expect_silent(
    create_table_from_data_table(
      conn,
      test_data,
      "test_table",
      schema = test_config$schema,
      if_not_exists = TRUE
    )
  )

  # Clean up
  drop_table(conn, "test_table")
  disconnect_from_db(conn)
})

test_that("create_table_from_data_table handles errors correctly", {
  skip_if_not(rpostgres_available, "RPostgres not available")
  # Skip if database not available
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
    },
    error = function(e) {
      skip("Database not available for testing")
    }
  )

  # Create test connection
  config <- configure_timescale(
    host = test_config$host,
    port = test_config$port,
    db = test_config$dbname,
    user = test_config$user,
    pass = test_config$password
  )
  conn <- connect_to_db(config)

  # Test invalid connection
  invalid_conn <- list()
  class(invalid_conn) <- "invalid_connection"
  expect_error(
    create_table_from_data_table(
      invalid_conn,
      data.table::data.table(),
      "test_table"
    )
  )

  # Test invalid data.table
  expect_error(
    create_table_from_data_table(
      conn,
      data.frame(),
      "test_table"
    )
  )

  # Test empty data.table
  expect_error(
    create_table_from_data_table(
      conn,
      data.table::data.table(),
      "test_table"
    )
  )

  # Test invalid time column
  test_data <- data.table::data.table(
    time = seq(as.POSIXct("2024-01-01"), by = "1 hour", length.out = 24),
    value = rnorm(24)
  )

  expect_error(
    create_table_from_data_table(
      conn,
      test_data,
      "test_table",
      as_hypertable = TRUE,
      time_column = "invalid_column"
    )
  )

  disconnect_from_db(conn)
})

test_that("append_data_table_to_table works correctly", {
  skip_if_not(rpostgres_available, "RPostgres not available")
  # Skip if database not available
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
    },
    error = function(e) {
      skip("Database not available for testing")
    }
  )

  # Create test data
  test_data <- data.table::data.table(
    time = seq(as.POSIXct("2024-01-01"), by = "1 hour", length.out = 24),
    value = rnorm(24)
  )

  # Create test connection
  config <- configure_timescale(
    host = test_config$host,
    port = test_config$port,
    db = test_config$dbname,
    user = test_config$user,
    pass = test_config$password,
    schema = test_config$schema
  )
  conn <- connect_to_db(config)

  # Create table
  create_table_from_data_table(
    conn,
    test_data,
    "test_table",
    primary_keys = "time",
    date_time_columns = "time"
  )

  # Test appending data
  expect_silent(
    append_data_table_to_table(
      conn,
      test_data,
      "test_table"
    )
  )

  # Clean up
  drop_table(conn, "test_table")
  disconnect_from_db(conn)
})

test_that("table operations handle errors correctly", {
  skip_if_not(rpostgres_available, "RPostgres not available")
  # Skip if database not available
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
    },
    error = function(e) {
      skip("Database not available for testing")
    }
  )

  # Create test connection
  config <- configure_timescale(
    host = test_config$host,
    port = test_config$port,
    db = test_config$dbname,
    user = test_config$user,
    pass = test_config$password
  )
  conn <- connect_to_db(config)

  # Test non-existent table
  expect_false(table_exists(conn, "non_existent_table"))

  # Test dropping non-existent table
  expect_error(drop_table(conn, "non_existent_table"))

  # Test renaming non-existent table
  expect_error(rename_table(conn, "non_existent_table", "new_table"))

  disconnect_from_db(conn)
})
