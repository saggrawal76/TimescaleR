## ----include = FALSE----------------------------------------------------------
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>",
  eval = FALSE  # Don't evaluate code chunks since they require database connection
)

## ----setup--------------------------------------------------------------------
#  library(TimescaleR)

## -----------------------------------------------------------------------------
#  # Initialize a global connection
#  init_connection(
#    host = "localhost",
#    port = 5432,
#    db = "your_database",
#    user = "your_username",
#    pass = "your_password",
#    schema = "public"
#  )
#  
#  # Check if connection is established
#  has_connection()

## -----------------------------------------------------------------------------
#  # Create sample time-series data
#  data <- data.frame(
#    time = seq(as.POSIXct("2023-01-01"), by = "1 hour", length.out = 1000),
#    sensor_id = rep(c("sensor_1", "sensor_2"), each = 500),
#    temperature = rnorm(1000, 20, 5),
#    humidity = rnorm(1000, 60, 10)
#  )
#  
#  # Create a hypertable from the data
#  create_table_from_data_table(
#    data,
#    "sensor_readings",
#    as_hypertable = TRUE,
#    time_column = "time"
#  )

## -----------------------------------------------------------------------------
#  # Get hourly averages
#  hourly_avg <- time_bucket(
#    table_name = "sensor_readings",
#    time_column = "time",
#    bucket_interval = "1 hour",
#    value_column = "temperature"
#  )
#  
#  # Get daily averages grouped by sensor
#  daily_avg <- time_bucket(
#    table_name = "sensor_readings",
#    time_column = "time",
#    bucket_interval = "1 day",
#    value_column = c("temperature", "humidity"),
#    group_by = "sensor_id"
#  )

## -----------------------------------------------------------------------------
#  # Get the first and last readings
#  first_reading <- first_value(
#    table_name = "sensor_readings",
#    value_column = "temperature",
#    time_column = "time"
#  )
#  
#  last_reading <- last_value(
#    table_name = "sensor_readings",
#    value_column = "temperature",
#    time_column = "time"
#  )
#  
#  # Calculate moving averages
#  moving_avg <- moving_average(
#    table_name = "sensor_readings",
#    value_column = "temperature",
#    time_column = "time",
#    window_size = 7
#  )

## -----------------------------------------------------------------------------
#  # Create a continuous aggregate for daily statistics
#  create_continuous_aggregate(
#    view_name = "daily_sensor_stats",
#    table_name = "sensor_readings",
#    time_column = "time",
#    interval = "1 day",
#    select_expr = "time_bucket('1 day', time) AS day,
#                   sensor_id,
#                   AVG(temperature) AS avg_temp,
#                   MAX(temperature) AS max_temp,
#                   MIN(temperature) AS min_temp,
#                   AVG(humidity) AS avg_humidity"
#  )
#  
#  # Add automatic refresh policy
#  add_refresh_policy("daily_sensor_stats", "1 hour")

## -----------------------------------------------------------------------------
#  # Add compression policy - compress data older than 7 days
#  add_compression_policy("sensor_readings", "7 days")
#  
#  # Add retention policy - drop data older than 1 year
#  add_retention_policy("sensor_readings", "1 year")

## -----------------------------------------------------------------------------
#  # Backup a hypertable
#  backup_hypertable("sensor_readings", "/path/to/backup/")
#  
#  # Restore from backup
#  restore_hypertable("sensor_readings", "/path/to/backup/")

## -----------------------------------------------------------------------------
#  # Get some sample data
#  plot_data <- get_query("
#    SELECT time, temperature
#    FROM sensor_readings
#    WHERE sensor_id = 'sensor_1'
#    ORDER BY time
#    LIMIT 100
#  ")
#  
#  # Create a time-series plot
#  plot <- plot_timeseries(
#    data = plot_data,
#    time_col = "time",
#    value_col = "temperature",
#    title = "Temperature Over Time",
#    color = "blue"
#  )
#  
#  print(plot)

## -----------------------------------------------------------------------------
#  # Close the global connection
#  close_connection()

