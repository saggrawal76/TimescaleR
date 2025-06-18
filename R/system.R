#' Run database maintenance operations
#'
#' @param conn Database connection object (optional if global connection is initialized)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return TRUE if successful
#' @export
#' @examples
#' \dontrun{
#' # Initialize connection
#' init_connection(
#'   host = "localhost", 
#'   port = 5432,
#'   db = "testdb", 
#'   user = "testuser", 
#'   pass = "testpass"
#' )
#' 
#' # Using global connection
#' run_maintenance()
#' 
#' # Or with explicit connection
#' conn <- get_connection()
#' run_maintenance(conn)
#' 
#' # With debugging enabled
#' run_maintenance(debug = TRUE)
#' }
run_maintenance <- function(conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Running database maintenance operations\n")
  
  # Get connection if not provided
  if (is.null(conn)) {
    if (debug) cat("[DEBUG] Using global connection\n")
    conn <- get_connection()
  }

  if (!DBI::dbIsValid(conn)) {
    stop("Invalid database connection")
  }

  tryCatch(
    {
      # Run VACUUM ANALYZE on all tables
      if (debug) cat("[DEBUG] Executing VACUUM ANALYZE on all tables\n")
      DBI::dbExecute(conn, "VACUUM ANALYZE")
      if (debug) cat("[DEBUG] Maintenance completed successfully\n")
      return(TRUE)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error during maintenance operation:", e$message, "\n")
      stop(paste("Maintenance operation failed:", e$message))
    }
  )
}

#' Optimize database performance
#'
#' @param conn Database connection object (optional if global connection is initialized)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return TRUE if successful
#' @export
#' @examples
#' \dontrun{
#' # Initialize connection
#' init_connection(
#'   host = "localhost", 
#'   port = 5432,
#'   db = "testdb", 
#'   user = "testuser", 
#'   pass = "testpass"
#' )
#' 
#' # Using global connection
#' optimize_db()
#' 
#' # Or with explicit connection
#' conn <- get_connection()
#' optimize_db(conn)
#' 
#' # With debugging enabled
#' optimize_db(debug = TRUE)
#' }
optimize_db <- function(conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Optimizing database performance\n")
  
  # Get connection if not provided
  if (is.null(conn)) {
    if (debug) cat("[DEBUG] Using global connection\n")
    conn <- get_connection()
  }

  if (!DBI::dbIsValid(conn)) {
    stop("Invalid database connection")
  }

  tryCatch(
    {
      # Update statistics
      if (debug) cat("[DEBUG] Executing ANALYZE to update statistics\n")
      DBI::dbExecute(conn, "ANALYZE")

      # Check for bloat
      if (debug) cat("[DEBUG] Executing VACUUM to reclaim storage\n")
      DBI::dbExecute(conn, "VACUUM")

      if (debug) cat("[DEBUG] Database optimization completed successfully\n")
      return(TRUE)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error during database optimization:", e$message, "\n")
      stop(paste("Optimization failed:", e$message))
    }
  )
}

#' Check PostgreSQL settings
#'
#' @param conn Database connection object (optional if global connection is initialized)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return Data frame with PostgreSQL settings
#' @export
#' @examples
#' \dontrun{
#' # Initialize connection
#' init_connection(
#'   host = "localhost", 
#'   port = 5432,
#'   db = "testdb", 
#'   user = "testuser", 
#'   pass = "testpass"
#' )
#' 
#' # Using global connection
#' settings <- check_postgres_settings()
#' 
#' # Or with explicit connection
#' conn <- get_connection()
#' settings <- check_postgres_settings(conn)
#' 
#' # With debugging enabled
#' settings <- check_postgres_settings(debug = TRUE)
#' }
check_postgres_settings <- function(conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Checking PostgreSQL settings\n")
  
  # Get connection if not provided
  if (is.null(conn)) {
    if (debug) cat("[DEBUG] Using global connection\n")
    conn <- get_connection()
  }

  if (!DBI::dbIsValid(conn)) {
    stop("Invalid database connection")
  }

  tryCatch(
    {
      if (debug) cat("[DEBUG] Executing query to get PostgreSQL settings\n")
      result <- DBI::dbGetQuery(conn, "
      SELECT
        name,
        setting,
        unit,
        category,
        short_desc,
        extra_desc,
        context,
        vartype,
        source,
        min_val,
        max_val,
        enumvals,
        boot_val,
        reset_val,
        sourcefile,
        sourceline,
        pending_restart
      FROM pg_settings
      ORDER BY category, name
    ")
      if (debug) cat("[DEBUG] Retrieved", nrow(result), "PostgreSQL settings\n")
      return(result)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error checking PostgreSQL settings:", e$message, "\n")
      stop(paste("Failed to check PostgreSQL settings:", e$message))
    }
  )
}
