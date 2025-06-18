# Load required packages
library(testthat)
library(DBI)
library(RPostgres)
library(data.table)

context("Visualization Tests")

# Test configuration
test_config <- list(
  host = "localhost",
  port = 5432,
  dbname = "testdb",
  user = "testuser",
  password = "testpass",
  schema = "public"
)

test_that("visualization functions work correctly", {
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
    value = rnorm(24),
    group = rep(c("A", "B"), each = 12)
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

  # Get data for plotting
  plot_data <- TimescaleR::get_query("SELECT * FROM test_table ORDER BY time")

  # Test time series plot with data frame
  expect_silent(
    TimescaleR::plot_timeseries(
      data = plot_data,
      time_col = "time",
      value_col = "value",
      title = "Test Plot"
    )
  )

  # Test chunks plot
  expect_silent(
    TimescaleR::plot_chunks(
      table_name = "test_table"
    )
  )

  # Test compression ratio plot
  expect_silent(
    TimescaleR::plot_compression_ratio(
      table_name = "test_table"
    )
  )

  # Test query duration plot
  expect_silent(
    TimescaleR::plot_query_duration(
      table_name = "test_table"
    )
  )

  # Test dashboard stats
  stats <- TimescaleR::dashboard_stats(table_name = "test_table")
  expect_type(stats, "list")

  # Clean up
  TimescaleR::drop_table(table_name = "test_table")
  TimescaleR::close_connection()
})

test_that("visualization functions handle errors correctly", {
  # Test plot_timeseries input validation without database connection
  test_data <- data.frame(
    time = as.POSIXct(c("2024-01-01 12:00:00", "2024-01-01 13:00:00")),
    value = c(10.5, 20.3)
  )

  # Test with valid data
  plot <- TimescaleR::plot_timeseries(test_data, "time", "value", "Test Plot")
  expect_s3_class(plot, "ggplot")

  # Test error handling
  expect_error(
    TimescaleR::plot_timeseries("not a data frame", "time", "value"),
    "Data must be a data frame"
  )

  expect_error(
    TimescaleR::plot_timeseries(test_data, "nonexistent", "value"),
    "Time column nonexistent not found in data"
  )

  expect_error(
    TimescaleR::plot_timeseries(test_data, "time", "nonexistent"),
    "Value column nonexistent not found in data"
  )
})
