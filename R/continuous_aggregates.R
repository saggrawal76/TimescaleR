#' Create a continuous aggregate view
#'
#' Creates a continuous aggregate view in TimescaleDB which automatically maintains
#' a materialized view of an aggregate query on a hypertable. This is useful for
#' efficiently computing and retrieving aggregate data over time intervals.
#'
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param view_name The name of the continuous aggregate view to create
#' @param table_name The source hypertable name
#' @param time_column The name of the time column in the hypertable
#' @param interval The time bucket interval (e.g., '1 hour', '1 day')
#' @param select_expr The SQL expression for the SELECT statement (required)
#' @param where_clause Optional WHERE clause to filter source data
#' @param group_by Additional GROUP BY columns (time bucket is automatically included)
#' @param having_clause Optional HAVING clause for filtering aggregated data
#' @param with_data Whether to populate the view with data on creation (TRUE) or create an empty view (FALSE)
#' @param materialized_only Whether to make the view materialized only
#' @param create_materialized_view Force create a standard materialized view instead of continuous aggregate
#' @param debug Enable debug messages (optional, default FALSE)
#' @return TRUE if the view was created successfully
#' @export
#' @examples
#' \dontrun{
#' # Initialize connection
#' init_connection(
#'   host = "localhost", 
#'   port = 5432, 
#'   db = "testdb", 
#'   user = "testuser", 
#'   pass = "testpass", 
#'   schema = "public"
#' )
#' 
#' # Using global connection
#' create_continuous_aggregate(
#'   view_name = "metrics_hourly",
#'   table_name = "metrics",
#'   time_column = "timestamp",
#'   interval = '1 hour',
#'   select_expr = "time_bucket('1 hour', timestamp) AS hour, 
#'                  sensor_id, 
#'                  AVG(temperature) AS avg_temp,
#'                  MAX(temperature) AS max_temp"
#' )
#' 
#' # Or with explicit connection
#' conn <- get_connection()
#' create_continuous_aggregate(
#'   conn,
#'   view_name = "metrics_daily",
#'   table_name = "metrics",
#'   time_column = "timestamp",
#'   interval = '1 day',
#'   select_expr = "time_bucket('1 day', timestamp) AS day, 
#'                  sensor_id, 
#'                  AVG(temperature) AS avg_temp",
#'   where_clause = "temperature > 0"
#' )
#' }
create_continuous_aggregate <- function(conn = NULL, view_name, table_name, time_column, interval,
                                        select_expr, where_clause = NULL, group_by = NULL,
                                        having_clause = NULL, with_data = TRUE,
                                        materialized_only = FALSE, create_materialized_view = FALSE,
                                        debug = FALSE) {
  if (debug) cat("[DEBUG] Creating continuous aggregate view:", view_name, "\n")
  
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
      # Create the continuous aggregate
      if (debug) cat("[DEBUG] Executing query to create continuous aggregate\n")
      DBI::dbExecute(conn, paste("
      CREATE MATERIALIZED VIEW", view_name, "
      WITH (timescaledb.continuous) AS
      ", select_expr))

      # Add refresh policy
      if (debug) cat("[DEBUG] Adding refresh policy\n")
      add_refresh_policy(conn, view_name, interval, debug)

      if (debug) cat("[DEBUG] Continuous aggregate created successfully\n")
      return(TRUE)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error creating continuous aggregate:", e$message, "\n")
      stop(paste("Failed to create continuous aggregate:", e$message))
    }
  )
}

#' Refresh a continuous aggregate view
#'
#' Manually refreshes a continuous aggregate view for a specific time range.
#' This is useful when you need to ensure the view is up-to-date for a certain
#' period, or after bulk data changes.
#'
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param view_name The name of the continuous aggregate view to refresh
#' @param start_time The start of the time range to refresh (can be NULL for unbounded)
#' @param end_time The end of the time range to refresh (can be NULL for unbounded)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return TRUE if the refresh was successful
#' @export
#' @examples
#' \dontrun{
#' # Initialize connection
#' init_connection(
#'   host = "localhost", 
#'   port = 5432, 
#'   db = "testdb", 
#'   user = "testuser", 
#'   pass = "testpass", 
#'   schema = "public"
#' )
#' 
#' # Using global connection - refresh all data
#' refresh_continuous_aggregate("metrics_hourly")
#' 
#' # Using global connection - refresh specific range
#' refresh_continuous_aggregate(
#'   view_name = "metrics_hourly",
#'   start_time = "2023-01-01",
#'   end_time = "2023-01-31"
#' )
#' 
#' # Or with explicit connection
#' conn <- get_connection()
#' refresh_continuous_aggregate(conn, "metrics_hourly", "2023-01-01", "2023-01-31")
#' }
refresh_continuous_aggregate <- function(conn = NULL, view_name, start_time = NULL, end_time = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Refreshing continuous aggregate view:", view_name, "\n")
  
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
      # Build the refresh command
      refresh_cmd <- paste("CALL refresh_continuous_aggregate('", view_name, "'", sep = "")
      
      if (debug) {
        cat("[DEBUG] Building refresh command\n")
        if (!is.null(start_time)) cat("[DEBUG] Start time:", format(start_time, "%Y-%m-%d %H:%M:%S"), "\n")
        if (!is.null(end_time)) cat("[DEBUG] End time:", format(end_time, "%Y-%m-%d %H:%M:%S"), "\n")
      }

      if (!is.null(start_time)) {
        refresh_cmd <- paste(refresh_cmd, ", '", format(start_time, "%Y-%m-%d %H:%M:%S"), "'", sep = "")
      }

      if (!is.null(end_time)) {
        refresh_cmd <- paste(refresh_cmd, ", '", format(end_time, "%Y-%m-%d %H:%M:%S"), "'", sep = "")
      }

      refresh_cmd <- paste(refresh_cmd, ")", sep = "")
      
      if (debug) cat("[DEBUG] Executing refresh command:", refresh_cmd, "\n")

      # Execute refresh
      DBI::dbExecute(conn, refresh_cmd)
      
      if (debug) cat("[DEBUG] Continuous aggregate refreshed successfully\n")
      return(TRUE)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error refreshing continuous aggregate:", e$message, "\n")
      stop(paste("Failed to refresh continuous aggregate:", e$message))
    }
  )
}

#' Drop a continuous aggregate view
#'
#' Drops a continuous aggregate view from the database.
#'
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param view_name The name of the continuous aggregate view to drop
#' @param if_exists Only throw a warning if the view doesn't exist, instead of an error
#' @param cascade Also drop objects that depend on this view
#' @param debug Enable debug messages (optional, default FALSE)
#' @return TRUE if the view was dropped successfully
#' @export
#' @examples
#' \dontrun{
#' # Initialize connection
#' init_connection(
#'   host = "localhost", 
#'   port = 5432, 
#'   db = "testdb", 
#'   user = "testuser", 
#'   pass = "testpass", 
#'   schema = "public"
#' )
#' 
#' # Using global connection
#' drop_continuous_aggregate("metrics_hourly")
#' 
#' # Or with explicit connection 
#' conn <- get_connection()
#' drop_continuous_aggregate(conn, "metrics_hourly", if_exists = TRUE, cascade = TRUE)
#' }
drop_continuous_aggregate <- function(conn = NULL, view_name, if_exists = FALSE, cascade = FALSE, debug = FALSE) {
  if (debug) cat("[DEBUG] Dropping continuous aggregate view:", view_name, "\n")
  
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
      if (debug) cat("[DEBUG] Executing query to drop continuous aggregate\n")
      DBI::dbExecute(conn, paste("DROP MATERIALIZED VIEW", view_name))
      if (debug) cat("[DEBUG] Continuous aggregate dropped successfully\n")
      return(TRUE)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error dropping continuous aggregate:", e$message, "\n")
      stop(paste("Failed to drop continuous aggregate:", e$message))
    }
  )
}

#' List continuous aggregate views
#'
#' Lists all continuous aggregate views defined in the connected database.
#'
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param pattern Optional pattern to filter view names
#' @param schema Optional schema name to filter views
#' @param include_definition Whether to include the view definition
#' @param debug Enable debug messages (optional, default FALSE)
#' @return Data frame with information about the continuous aggregate views
#' @export
#' @examples
#' \dontrun{
#' # Initialize connection
#' init_connection(
#'   host = "localhost", 
#'   port = 5432, 
#'   db = "testdb", 
#'   user = "testuser", 
#'   pass = "testpass", 
#'   schema = "public"
#' )
#' 
#' # Using global connection
#' views <- list_continuous_aggregates()
#' print(views)
#' 
#' # Or with explicit connection and pattern
#' conn <- get_connection()
#' metrics_views <- list_continuous_aggregates(conn, pattern = "metrics_%", include_definition = TRUE)
#' }
list_continuous_aggregates <- function(conn = NULL, pattern = NULL, schema = NULL, include_definition = FALSE, debug = FALSE) {
  if (debug) cat("[DEBUG] Listing continuous aggregates\n")
  
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
      if (debug) cat("[DEBUG] Executing query to list continuous aggregates\n")
      result <- DBI::dbGetQuery(conn, "
      SELECT
        view_name,
        view_definition,
        refresh_lag,
        refresh_interval,
        max_interval_per_job,
        materialization_hypertable,
        view_owner
      FROM timescaledb_information.continuous_aggregates
      ORDER BY view_name
    ")
      if (debug) cat("[DEBUG] Retrieved", nrow(result), "continuous aggregates\n")
      return(result)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error listing continuous aggregates:", e$message, "\n")
      stop(paste("Failed to list continuous aggregates:", e$message))
    }
  )
}

#' Get continuous aggregate policy
#'
#' Retrieves the refresh policy for a continuous aggregate view.
#'
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param view_name The name of the continuous aggregate view
#' @param debug Enable debug messages (optional, default FALSE)
#' @return Data frame with policy information or NULL if no policy exists
#' @export
#' @examples
#' \dontrun{
#' # Initialize connection
#' init_connection(
#'   host = "localhost", 
#'   port = 5432, 
#'   db = "testdb", 
#'   user = "testuser", 
#'   pass = "testpass", 
#'   schema = "public"
#' )
#' 
#' # Using global connection
#' policy <- get_continuous_aggregate_policy("metrics_hourly")
#' print(policy)
#' 
#' # Or with explicit connection
#' conn <- get_connection()
#' policy <- get_continuous_aggregate_policy(conn, "metrics_hourly")
#' }
get_continuous_aggregate_policy <- function(conn = NULL, view_name, debug = FALSE) {
  if (debug) cat("[DEBUG] Retrieving continuous aggregate policy for:", view_name, "\n")
  
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
      if (debug) cat("[DEBUG] Executing query to get continuous aggregate policy\n")
      result <- DBI::dbGetQuery(conn, paste("
      SELECT
        start_offset,
        end_offset,
        schedule_interval
      FROM timescaledb_information.continuous_aggregate_policies
      WHERE view_name = '", view_name, "'", sep = ""))
      if (debug) cat("[DEBUG] Retrieved policy for", view_name, "\n")
      return(result)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error retrieving continuous aggregate policy:", e$message, "\n")
      return(NULL)
    }
  )
}

#' Add a refresh policy to a continuous aggregate
#'
#' Adds an automatic refresh policy to a continuous aggregate view. This schedules
#' regular updates to keep the view in sync with the underlying data.
#'
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param view_name The name of the continuous aggregate view
#' @param refresh_interval The interval at which the view should be refreshed (e.g., '1 hour', '1 day')
#' @param start_offset Optional time interval specifying how far back to refresh (default NULL)
#' @param end_offset Optional time interval specifying how recent the data should be (default NULL)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return TRUE if the policy was added successfully
#' @export
#' @examples
#' \dontrun{
#' # Initialize connection
#' init_connection(
#'   host = "localhost", 
#'   port = 5432, 
#'   db = "testdb", 
#'   user = "testuser", 
#'   pass = "testpass", 
#'   schema = "public"
#' )
#' 
#' # Using global connection with explicit connection object
#' conn <- get_connection()
#' add_refresh_policy(conn, "metrics_hourly", "1 hour")
#' 
#' # Or with custom offsets
#' add_refresh_policy(
#'   conn,
#'   "metrics_daily", 
#'   "1 day",
#'   start_offset = "7 days",
#'   end_offset = "1 hour"
#' )
#' }
add_refresh_policy <- function(conn = NULL, view_name, refresh_interval, 
                              start_offset = NULL, end_offset = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Adding refresh policy to continuous aggregate:", view_name, "\n")
  
  # Handle the case where view_name is passed as first parameter
  if (is.character(conn) && is.null(view_name)) {
    if (debug) cat("[DEBUG] First parameter is a string, assuming it's the view_name\n")
    view_name <- conn
    conn <- NULL
  }
  
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
      # Build the add policy command
      policy_cmd <- paste("SELECT add_continuous_aggregate_policy('", view_name, "',", sep = "")
      
      # Add start_offset if provided
      if (!is.null(start_offset)) {
        if (debug) cat("[DEBUG] Using start_offset:", start_offset, "\n")
        policy_cmd <- paste(policy_cmd, " start_offset => INTERVAL '", start_offset, "',", sep = "")
      }
      
      # Add end_offset if provided
      if (!is.null(end_offset)) {
        if (debug) cat("[DEBUG] Using end_offset:", end_offset, "\n")
        policy_cmd <- paste(policy_cmd, " end_offset => INTERVAL '", end_offset, "',", sep = "")
      }
      
      # Add refresh interval
      if (debug) cat("[DEBUG] Using refresh_interval:", refresh_interval, "\n")
      policy_cmd <- paste(policy_cmd, " schedule_interval => INTERVAL '", refresh_interval, "')", sep = "")
      
      if (debug) cat("[DEBUG] Executing command:", policy_cmd, "\n")
      
      # Execute add policy command
      DBI::dbExecute(conn, policy_cmd)
      
      if (debug) cat("[DEBUG] Refresh policy added successfully\n")
      return(TRUE)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error adding refresh policy:", e$message, "\n")
      stop(paste("Failed to add refresh policy:", e$message))
    }
  )
}
