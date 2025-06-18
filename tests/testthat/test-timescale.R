# Load required packages
library(pacman)
p_load(testthat, DBI, RPostgres, data.table)

context("TimescaleDB Tests")

# Test configuration
test_config <- list(
  host = "localhost",
  port = 5432,
  dbname = "testdb",
  user = "testuser",
  password = "testpass",
  schema = "public"
)

test_that("hypertable operations work correctly", {
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
  test_data <- data.table::data.table(
    time = seq(as.POSIXct("2024-01-01"), by = "1 hour", length.out = 24),
    value = rnorm(24)
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

  # Create table
  TimescaleR::create_table_from_data_table(
    data_table = test_data,
    table_name = "test_table",
    primary_keys = "time",
    date_time_columns = "time"
  )

  # Test hypertable conversion
  expect_silent(
    TimescaleR::convert_to_hypertable(
      table_name = "test_table",
      time_column = "time",
      chunk_time_interval = "1 day"
    )
  )

  # Verify hypertable status
  expect_true(TimescaleR::is_hypertable(table_name = "test_table"))

  # Test compression policy
  expect_silent(
    TimescaleR::add_compression_policy(
      table_name = "test_table",
      compress_after = "7 days"
    )
  )

  # Test retention policy
  expect_silent(
    TimescaleR::add_retention_policy(
      table_name = "test_table",
      drop_after = "1 year"
    )
  )

  # Get chunks info
  chunks_info <- TimescaleR::get_chunks_info(table_name = "test_table")
  expect_type(chunks_info, "list")

  # Show policies
  policies <- TimescaleR::show_policies(table_name = "test_table")
  expect_type(policies, "list")

  # Clean up
  TimescaleR::drop_table(table_name = "test_table")
  TimescaleR::close_connection()
})

test_that("continuous aggregates work correctly", {
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
  test_data <- data.table::data.table(
    time = seq(as.POSIXct("2024-01-01"), by = "1 hour", length.out = 24),
    value = rnorm(24)
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

  # Create table and convert to hypertable
  TimescaleR::create_table_from_data_table(
    data_table = test_data,
    table_name = "test_table",
    primary_keys = "time",
    date_time_columns = "time",
    as_hypertable = TRUE,
    time_column = "time"
  )

  # Create continuous aggregate
  expect_silent(
    TimescaleR::create_continuous_aggregate(
      view_name = "test_agg",
      query = "SELECT time_bucket('1 day', time) as bucket, avg(value) as avg_value FROM test_table GROUP BY bucket"
    )
  )

  # List continuous aggregates
  aggs <- TimescaleR::list_continuous_aggregates()
  expect_type(aggs, "list")

  # Refresh continuous aggregate
  expect_silent(
    TimescaleR::refresh_continuous_aggregate(
      view_name = "test_agg"
    )
  )

  # Clean up
  TimescaleR::drop_continuous_aggregate(view_name = "test_agg")
  TimescaleR::drop_table(table_name = "test_table")
  TimescaleR::close_connection()
})
