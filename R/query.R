#' Execute a SQL Query
#'
#' Executes a SQL query against a TimescaleDB database and returns the results.
#' This function is flexible and can be called in multiple ways:
#' - With a connection and query: get_query(conn, query, ...)
#' - With just a query (using global connection): get_query(query, ...)
#' - With named parameters: get_query(query = "SELECT ...", params = list(...))
#'
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param query SQL query string
#' @param params Optional parameters for prepared statements
#' @param auto_reconnect Whether to attempt reconnection if the connection is lost (default: TRUE)
#' @param debug Enable verbose debug output (default: FALSE)
#' @return Query results as a data frame
#' @export
#' @examples
#' \dontrun{
#' # Initialize global connection
#' init_connection(
#'   host = "localhost", port = 5432,
#'   db = "testdb", user = "testuser", pass = "testpass"
#' )
#' 
#' # Using an explicit connection
#' conn <- get_connection()
#' # Basic query with explicit connection
#' result <- get_query(conn, "SELECT * FROM my_table LIMIT 10")
#' # Query with parameters and explicit connection
#' result <- get_query(
#'   conn,
#'   "SELECT * FROM my_table WHERE value > $1 AND time > $2",
#'   params = list(100, "2023-01-01")
#' )
#' 
#' # Using global connection (common usage)
#' # Simple query with global connection
#' result <- get_query("SELECT * FROM my_table LIMIT 10")
#' # Query with named parameters
#' result <- get_query(
#'   query = "SELECT * FROM my_table WHERE value > $1 AND time > $2",
#'   params = list(100, "2023-01-01")
#' )
#' 
#' # Shortened syntax for global connection
#' temps <- get_query("SELECT time, temperature FROM weather_data")
#' 
#' # Always close the connection when done
#' close_connection()
#' }
get_query <- function(conn = NULL, query, params = NULL, auto_reconnect = TRUE, debug = FALSE) {
  if (debug) {
    message("get_query: Starting execution")
    message("get_query: Query = ", substr(query, 1, 50), if(nchar(query) > 50) "..." else "")
    if (!is.null(params)) {
      message("get_query: With parameters")
    }
  }
  
  # Get connection with automatic reconnection if needed
  conn <- ensure_valid_connection(auto_reconnect, debug)
  
  if (debug) message("get_query: Got valid connection, executing query")
  
  tryCatch(
    {
      if (is.null(params)) {
        if (debug) message("get_query: Executing simple query")
        result <- DBI::dbGetQuery(conn, query)
      } else {
        if (debug) message("get_query: Executing parameterized query")
        result <- DBI::dbGetQuery(conn, query, params = params)
      }
      
      if (debug) {
        message("get_query: Query executed successfully")
        message("get_query: Result has ", nrow(result), " rows and ", ncol(result), " columns")
      }
      
      return(result)
    },
    error = function(e) {
      error_msg <- paste("Query execution failed:", e$message)
      if (debug) message("get_query: ERROR - ", error_msg)
      stop(error_msg)
    }
  )
}

#' Time bucket query
#'
#' Executes a TimescaleDB time_bucket query to aggregate time series data into regular intervals.
#' This is one of the core time-series operations in TimescaleDB.
#' Uses the global database connection initialized with init_connection().
#'
#' @param table_name Name of the table
#' @param time_column Name of the time column
#' @param bucket_interval Interval for bucketing (e.g., '1 hour', '1 day')
#' @param bucket_width Alternative name for bucket_interval (for backward compatibility)
#' @param value_column Name of the value column (optional)
#' @param aggregates Custom aggregation expressions (e.g., "avg(value) as avg_value, count(*) as count")
#' @param group_by Additional columns to group by (optional)
#' @param where_clause WHERE clause (optional)
#' @param auto_reconnect Whether to attempt reconnection if the connection is lost (default: TRUE)
#' @return Query results as a data frame with time buckets and aggregated values
#' @export
#' @examples
#' \dontrun{
#' # Initialize global connection
#' init_connection(
#'   host = "localhost", port = 5432,
#'   db = "testdb", user = "testuser", pass = "testpass"
#' )
#' 
#' # Simple time bucket query - hourly average temperature
#' result <- time_bucket(
#'   table_name = "weather_data",
#'   time_column = "timestamp",
#'   bucket_interval = "1 hour",
#'   value_column = "temperature"
#' )
#' 
#' # Time bucket with custom aggregates
#' result <- time_bucket(
#'   table_name = "sensor_data",
#'   time_column = "time",
#'   bucket_width = "30 minutes",
#'   aggregates = "avg(temperature) as avg_temp, max(humidity) as max_humidity",
#'   where_clause = "sensor_id = 'ABC123' AND time > '2023-01-01'"
#' )
#' 
#' # Time bucket with grouping
#' result <- time_bucket(
#'   table_name = "metrics",
#'   time_column = "timestamp",
#'   bucket_interval = "1 day",
#'   value_column = "value",
#'   group_by = c("device_id", "metric_type")
#' )
#' 
#' # Always close the connection when done
#' close_connection()
#' }
time_bucket <- function(table_name, time_column, bucket_interval = NULL, bucket_width = NULL,
                        value_column = NULL, aggregates = NULL, group_by = NULL, where_clause = NULL,
                        auto_reconnect = TRUE) {
  # Handle backward compatibility - bucket_width is an alias for bucket_interval
  if (is.null(bucket_interval) && !is.null(bucket_width)) {
    bucket_interval <- bucket_width
  } else if (is.null(bucket_interval) && is.null(bucket_width)) {
    stop("Either bucket_interval or bucket_width must be specified")
  }
  
  # Get connection with automatic reconnection if needed
  conn <- ensure_valid_connection(auto_reconnect)

  tryCatch(
    {
      # Build query
      select_clause <- paste("time_bucket('", bucket_interval, "', ", time_column, ") as time_bucket", sep = "")

      # Add custom aggregates or default value columns
      if (!is.null(aggregates)) {
        select_clause <- paste(select_clause, ", ", aggregates, sep = "")
      } else if (!is.null(value_column)) {
        for (col in value_column) {
          select_clause <- paste(select_clause, ", AVG(", col, ") as avg_", col, sep = "")
        }
      }

      # Handle group by
      group_clause <- "time_bucket"
      if (!is.null(group_by)) {
        select_clause <- paste(select_clause, ", ", paste(group_by, collapse = ", "), sep = "")
        group_clause <- paste(group_clause, ", ", paste(group_by, collapse = ", "), sep = "")
      }

      query <- paste("SELECT ", select_clause, " FROM ", table_name, sep = "")

      if (!is.null(where_clause)) {
        query <- paste(query, " WHERE ", where_clause, sep = "")
      }

      query <- paste(query, " GROUP BY ", group_clause, " ORDER BY time_bucket", sep = "")

      # Execute query
      DBI::dbGetQuery(conn, query)
    },
    error = function(e) {
      stop(paste("Time bucket query failed:", e$message))
    }
  )
}

#' Get the last value in a time series
#'
#' Retrieves the last (most recent) value from a time series, based on the maximum
#' value of the specified time column.
#' Uses the global database connection initialized with init_connection().
#'
#' @param table_name Name of the table
#' @param value_column Name of the value column
#' @param time_column Name of the time column
#' @param where_clause Optional WHERE clause
#' @param auto_reconnect Whether to attempt reconnection if the connection is lost (default: TRUE)
#' @return The last value as a data frame
#' @export
#' @examples
#' \dontrun{
#' # Initialize global connection
#' init_connection(
#'   host = "localhost", port = 5432,
#'   db = "testdb", user = "testuser", pass = "testpass"
#' )
#' 
#' # Get the last temperature reading
#' last_temp <- last_value(
#'   table_name = "weather_data",
#'   value_column = "temperature",
#'   time_column = "timestamp"
#' )
#' 
#' # Get the last reading with a condition
#' last_reading <- last_value(
#'   table_name = "sensor_data",
#'   value_column = "reading",
#'   time_column = "time",
#'   where_clause = "sensor_id = 'ABC123'"
#' )
#' 
#' # Always close the connection when done
#' close_connection()
#' }
last_value <- function(table_name, value_column, time_column, where_clause = NULL, 
                      auto_reconnect = TRUE) {
  # Get connection with automatic reconnection if needed
  conn <- ensure_valid_connection(auto_reconnect)

  tryCatch(
    {
      query <- paste(
        "
      SELECT
        ", value_column, "
      FROM", table_name, "
      WHERE", time_column, "= (SELECT MAX(", time_column, ") FROM", table_name,
        if (!is.null(where_clause)) paste("WHERE", where_clause) else "", ")"
      )

      DBI::dbGetQuery(conn, query)
    },
    error = function(e) {
      stop(paste("Last value query failed:", e$message))
    }
  )
}

#' Get the first value in a time series
#'
#' Retrieves the first (earliest) value from a time series, based on the minimum
#' value of the specified time column.
#' Uses the global database connection initialized with init_connection().
#'
#' @param table_name Name of the table
#' @param value_column Name of the value column
#' @param time_column Name of the time column
#' @param where_clause Optional WHERE clause
#' @param auto_reconnect Whether to attempt reconnection if the connection is lost (default: TRUE)
#' @return The first value as a data frame
#' @export
#' @examples
#' \dontrun{
#' # Initialize global connection
#' init_connection(
#'   host = "localhost", port = 5432,
#'   db = "testdb", user = "testuser", pass = "testpass"
#' )
#' 
#' # Get the first temperature reading
#' first_temp <- first_value(
#'   table_name = "weather_data",
#'   value_column = "temperature",
#'   time_column = "timestamp"
#' )
#' 
#' # Get the first reading with a condition
#' first_reading <- first_value(
#'   table_name = "sensor_data",
#'   value_column = "reading",
#'   time_column = "time",
#'   where_clause = "sensor_id = 'ABC123' AND time > '2023-01-01'"
#' )
#' 
#' # Always close the connection when done
#' close_connection()
#' }
first_value <- function(table_name, value_column, time_column, where_clause = NULL,
                       auto_reconnect = TRUE) {
  # Get connection with automatic reconnection if needed
  conn <- ensure_valid_connection(auto_reconnect)

  tryCatch(
    {
      query <- paste(
        "
      SELECT
        ", value_column, "
      FROM", table_name, "
      WHERE", time_column, "= (SELECT MIN(", time_column, ") FROM", table_name,
        if (!is.null(where_clause)) paste("WHERE", where_clause) else "", ")"
      )

      DBI::dbGetQuery(conn, query)
    },
    error = function(e) {
      stop(paste("First value query failed:", e$message))
    }
  )
}

#' Calculate time-weighted average of a value over time
#'
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param table_name Name of the table
#' @param value_column Name of the value column to interpolate
#' @param time_column Name of the time column
#' @param where_clause Optional WHERE clause
#' @param debug Enable debug messages (optional, default FALSE)
#' @return Time-weighted average result
#' @export
#'
#' @examples
#' \dontrun{
#' # Using global connection
#' init_connection(
#'   host = "localhost", 
#'   port = 5432, 
#'   db = "testdb", 
#'   user = "testuser", 
#'   pass = "testpass", 
#'   schema = "public"
#' )
#' twa <- time_weighted_average(
#'   table_name = "sensor_data",
#'   value_column = "temperature",
#'   time_column = "timestamp"
#' )
#' 
#' # With a filter condition
#' twa_filtered <- time_weighted_average(
#'   table_name = "sensor_data",
#'   value_column = "temperature",
#'   time_column = "timestamp",
#'   where_clause = "device_id = 'dev001'"
#' )
#' 
#' # Or with explicit connection
#' conn <- get_connection()
#' twa <- time_weighted_average(
#'   conn,
#'   "sensor_data",
#'   "temperature",
#'   "timestamp"
#' )
#' }
time_weighted_average <- function(conn = NULL, table_name, value_column, time_column, where_clause = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Calculating time-weighted average for", value_column, "in table", table_name, "\n")
  
  # Use global connection if not provided
  if (is.null(conn)) {
    if (debug) cat("[DEBUG] Using global connection\n")
    conn <- get_connection()
  }

  if (!DBI::dbIsValid(conn)) {
    stop("Invalid database connection")
  }

  tryCatch(
    {
      # Build query
      query <- paste("
        SELECT time_weight(", time_column, ", ", value_column, ") AS time_weighted_average
        FROM ", table_name, "
      ", sep = "")

      if (!is.null(where_clause)) {
        if (debug) cat("[DEBUG] Adding WHERE clause:", where_clause, "\n")
        query <- paste(query, " WHERE ", where_clause, sep = "")
      }
      
      if (debug) cat("[DEBUG] Executing query for time-weighted average\n")

      # Execute query
      result <- DBI::dbGetQuery(conn, query)
      
      if (debug) cat("[DEBUG] Time-weighted average calculation completed\n")
      return(result)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error calculating time-weighted average:", e$message, "\n")
      stop(paste("Time-weighted average calculation failed:", e$message))
    }
  )
}

#' Calculate moving average of a value over time
#'
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param table_name Name of the table
#' @param value_column Name of the value column
#' @param time_column Name of the time column
#' @param window_size Size of the moving average window
#' @param where_clause Optional WHERE clause
#' @param debug Enable debug messages (optional, default FALSE)
#' @return Moving average result as a data frame
#' @export
#'
#' @examples
#' \dontrun{
#' # Using global connection
#' init_connection(
#'   host = "localhost", 
#'   port = 5432, 
#'   db = "testdb", 
#'   user = "testuser", 
#'   pass = "testpass", 
#'   schema = "public"
#' )
#' ma <- moving_average(
#'   table_name = "sensor_data",
#'   value_column = "temperature",
#'   time_column = "timestamp",
#'   window_size = 5
#' )
#' 
#' # Weekly moving average
#' ma_weekly <- moving_average(
#'   table_name = "daily_metrics",
#'   value_column = "value",
#'   time_column = "date",
#'   window_size = 7,
#'   where_clause = "metric_type = 'temperature'"
#' )
#' 
#' # Or with explicit connection
#' conn <- get_connection()
#' ma <- moving_average(
#'   conn,
#'   "sensor_data",
#'   "temperature",
#'   "timestamp"
#' )
#' }
moving_average <- function(conn = NULL, table_name, value_column, time_column, window_size = 3, where_clause = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Calculating moving average for", value_column, "in table", table_name, "with window size", window_size, "\n")
  
  # Use global connection if not provided
  if (is.null(conn)) {
    if (debug) cat("[DEBUG] Using global connection\n")
    conn <- get_connection()
  }

  if (!DBI::dbIsValid(conn)) {
    stop("Invalid database connection")
  }

  tryCatch(
    {
      # Build query
      query <- paste("
        SELECT ", time_column, ",
               AVG(", value_column, ") OVER (
                 ORDER BY ", time_column, "
                 ROWS BETWEEN ", window_size - 1, " PRECEDING AND CURRENT ROW
               ) AS moving_avg
        FROM ", table_name, "
      ", sep = "")

      if (!is.null(where_clause)) {
        if (debug) cat("[DEBUG] Adding WHERE clause:", where_clause, "\n")
        query <- paste(query, " WHERE ", where_clause, sep = "")
      }

      query <- paste(query, " ORDER BY ", time_column, sep = "")
      
      if (debug) cat("[DEBUG] Executing query for moving average\n")

      # Execute query
      result <- DBI::dbGetQuery(conn, query)
      
      if (debug) cat("[DEBUG] Moving average calculation completed, returned", nrow(result), "rows\n")
      return(result)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error calculating moving average:", e$message, "\n")
      stop(paste("Moving average calculation failed:", e$message))
    }
  )
}

#' Interpolate time series data to regular intervals
#'
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param table_name Name of the table
#' @param value_column Name of the value column to interpolate
#' @param time_column Name of the time column
#' @param interval Time interval for interpolation (e.g., '1 hour', '30 minutes')
#' @param where_clause Optional WHERE clause
#' @param debug Enable debug messages (optional, default FALSE)
#' @return Interpolated time series as a data frame
#' @export
#'
#' @examples
#' \dontrun{
#' # Using global connection
#' init_connection(
#'   host = "localhost", 
#'   port = 5432, 
#'   db = "testdb", 
#'   user = "testuser", 
#'   pass = "testpass", 
#'   schema = "public"
#' )
#' 
#' # Interpolate temperature readings to hourly intervals
#' hourly_temps <- interpolate(
#'   table_name = "sensor_data",
#'   value_column = "temperature",
#'   time_column = "timestamp",
#'   interval = "1 hour"
#' )
#' 
#' # Interpolate with a filter
#' filtered_data <- interpolate(
#'   table_name = "sensor_data",
#'   value_column = "humidity",
#'   time_column = "timestamp",
#'   interval = "30 minutes", 
#'   where_clause = "device_id = 'dev001'"
#' )
#' 
#' # Or with explicit connection
#' conn <- get_connection()
#' # Interpolate with global connection
#' daily_data <- interpolate(
#'   conn,
#'   "metrics",
#'   "value",
#'   "time",
#'   "1 day"
#' )
#' }
interpolate <- function(conn = NULL, table_name, value_column, time_column, interval = "1 hour", where_clause = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Interpolating", value_column, "in table", table_name, "with interval", interval, "\n")
  
  # Use global connection if not provided
  if (is.null(conn)) {
    if (debug) cat("[DEBUG] Using global connection\n")
    conn <- get_connection()
  }

  if (!DBI::dbIsValid(conn)) {
    stop("Invalid database connection")
  }

  tryCatch(
    {
      # Get the time range
      range_query <- paste("
        SELECT MIN(", time_column, ") AS min_time, 
               MAX(", time_column, ") AS max_time
        FROM ", table_name, "
      ", sep = "")

      if (!is.null(where_clause)) {
        if (debug) cat("[DEBUG] Adding WHERE clause to range query:", where_clause, "\n")
        range_query <- paste(range_query, " WHERE ", where_clause, sep = "")
      }
      
      if (debug) cat("[DEBUG] Executing range query to determine time boundaries\n")
      time_range <- DBI::dbGetQuery(conn, range_query)

      # Build interpolation query
      query <- paste("
        WITH time_series AS (
          SELECT generate_series(
            '", time_range$min_time, "'::timestamp,
            '", time_range$max_time, "'::timestamp,
            '", interval, "'::interval
          ) AS time_point
        ),
        data AS (
          SELECT ", time_column, ", ", value_column, "
          FROM ", table_name, "
      ", sep = "")

      if (!is.null(where_clause)) {
        if (debug) cat("[DEBUG] Adding WHERE clause to main query:", where_clause, "\n")
        query <- paste(query, " WHERE ", where_clause, sep = "")
      }

      query <- paste(query, "
        )
        SELECT 
          time_point,
          CASE
            WHEN EXISTS (SELECT 1 FROM data WHERE ", time_column, " = time_point)
            THEN (SELECT ", value_column, " FROM data WHERE ", time_column, " = time_point)
            ELSE (
              SELECT
                CASE
                  WHEN COUNT(*) = 2 THEN
                    ", value_column, "_before +
                    (time_point - time_before)::float / (time_after - time_before)::float *
                    (", value_column, "_after - ", value_column, "_before)
                  ELSE NULL
                END
              FROM (
                SELECT
                  MAX(", time_column, ") FILTER (WHERE ", time_column, " < time_point) AS time_before,
                  MIN(", time_column, ") FILTER (WHERE ", time_column, " > time_point) AS time_after,
                  MAX(", value_column, ") FILTER (WHERE ", time_column, " < time_point) AS ", value_column, "_before,
                  MIN(", value_column, ") FILTER (WHERE ", time_column, " > time_point) AS ", value_column, "_after,
                  COUNT(*) AS count
                FROM data
                WHERE ", time_column, " < time_point OR ", time_column, " > time_point
              ) AS bounds
            )
          END AS interpolated_value
        FROM time_series
        ORDER BY time_point
      ", sep = "")
      
      if (debug) cat("[DEBUG] Executing interpolation query\n")

      # Execute query
      result <- DBI::dbGetQuery(conn, query)
      
      if (debug) cat("[DEBUG] Interpolation completed, returned", nrow(result), "rows\n")
      return(result)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error interpolating time series:", e$message, "\n")
      stop(paste("Interpolation failed:", e$message))
    }
  )
}
