#' Get Database Size
#'
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return Database size in human-readable format
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
#' db_size <- get_db_size()
#' 
#' # Or with explicit connection
#' conn <- get_connection()
#' db_size <- get_db_size(conn)
#' }
get_db_size <- function(conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Getting database size\n")
  
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
      if (debug) cat("[DEBUG] Executing query to get database size\n")
      result <- DBI::dbGetQuery(conn, "
      SELECT
        pg_size_pretty(pg_database_size(current_database())) AS size
    ")
      if (debug) cat("[DEBUG] Database size:", result$size, "\n")
      return(result$size)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error getting database size:", e$message, "\n")
      stop(paste("Failed to get database size:", e$message))
    }
  )
}

#' Monitor Active Queries
#'
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return Data frame with active query information
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
#' active_queries <- monitor_active_queries()
#' 
#' # Or with explicit connection
#' conn <- get_connection()
#' active_queries <- monitor_active_queries(conn)
#' }
monitor_active_queries <- function(conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Monitoring active queries\n")
  
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
      if (debug) cat("[DEBUG] Executing query to get active queries\n")
      result <- DBI::dbGetQuery(conn, "
      SELECT
        pid,
        usename,
        application_name,
        client_addr,
        state,
        query_start,
        now() - query_start as duration,
        query
      FROM pg_stat_activity
      WHERE state != 'idle' AND pid <> pg_backend_pid()
      ORDER BY duration DESC
    ")
      if (debug) cat("[DEBUG] Retrieved", nrow(result), "active queries\n")
      return(result)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error monitoring active queries:", e$message, "\n")
      stop(paste("Failed to monitor active queries:", e$message))
    }
  )
}

#' Get Database Settings
#'
#' Retrieves important database settings that can affect TimescaleDB performance.
#'
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return Data frame with database settings
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
#' # Get database settings
#' settings <- get_db_settings()
#' print(settings)
#' 
#' # Or with explicit connection
#' conn <- get_connection()
#' settings <- get_db_settings(conn)
#' }
get_db_settings <- function(conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Getting database settings\n")
  
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
      if (debug) cat("[DEBUG] Executing query to get database settings\n")
      result <- DBI::dbGetQuery(conn, "
      SELECT
        name,
        setting,
        unit,
        context,
        short_desc
      FROM pg_settings
      ORDER BY name
    ")
      if (debug) cat("[DEBUG] Retrieved", nrow(result), "database settings\n")
      return(result)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error getting database settings:", e$message, "\n")
      stop(paste("Failed to get database settings:", e$message))
    }
  )
}

#' Monitor Hypertable Metrics
#'
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return Data frame with hypertable metrics
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
#' metrics <- monitor_hypertable_metrics()
#' 
#' # Or with explicit connection
#' conn <- get_connection()
#' metrics <- monitor_hypertable_metrics(conn)
#' }
monitor_hypertable_metrics <- function(conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Monitoring hypertable metrics\n")
  
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
      if (debug) cat("[DEBUG] Executing query to get hypertable metrics\n")
      result <- DBI::dbGetQuery(conn, "
      SELECT
        hypertable_schema,
        hypertable_name,
        num_chunks,
        num_dimensions,
        compression_enabled,
        is_distributed
      FROM timescaledb_information.hypertables
      ORDER BY hypertable_name
    ")
      if (debug) cat("[DEBUG] Retrieved metrics for", nrow(result), "hypertables\n")
      return(result)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error monitoring hypertable metrics:", e$message, "\n")
      stop(paste("Failed to monitor hypertable metrics:", e$message))
    }
  )
}

#' Get Database Version
#'
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return A list with PostgreSQL and TimescaleDB versions
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
#' version_info <- get_db_version()
#' 
#' # Or with explicit connection
#' conn <- get_connection()
#' version_info <- get_db_version(conn)
#' }
get_db_version <- function(conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Getting database version\n")
  
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
      if (debug) cat("[DEBUG] Executing query to get PostgreSQL version\n")
      pg_version <- DBI::dbGetQuery(conn, "SHOW server_version")$server_version

      if (debug) cat("[DEBUG] Executing query to get TimescaleDB version\n")
      ts_version_result <- tryCatch(
        {
          DBI::dbGetQuery(conn, "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
        },
        error = function(e) {
          if (debug) cat("[DEBUG] Error getting TimescaleDB version (may not be installed)\n")
          return(data.frame(extversion = NA))
        }
      )
      
      ts_version <- if (nrow(ts_version_result) > 0) ts_version_result$extversion else NA
      
      result <- list(
        postgresql = pg_version,
        timescaledb = ts_version
      )
      
      if (debug) {
        cat("[DEBUG] PostgreSQL version:", pg_version, "\n")
        cat("[DEBUG] TimescaleDB version:", if (!is.na(ts_version)) ts_version else "Not installed", "\n")
      }
      
      return(result)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error getting database version:", e$message, "\n")
      stop(paste("Failed to get database version:", e$message))
    }
  )
}

#' Monitor Database Locks
#'
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return Data frame with lock information
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
#' locks <- monitor_locks()
#' 
#' # Or with explicit connection
#' conn <- get_connection()
#' locks <- monitor_locks(conn)
#' }
monitor_locks <- function(conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Monitoring database locks\n")
  
  # If conn is explicitly provided but invalid, throw error immediately
  if (!missing(conn) && !is.null(conn) && !DBI::dbIsValid(conn)) {
    stop("Invalid database connection")
  }
  
  # If conn is explicitly NULL (passed as argument), treat as invalid
  if (!missing(conn) && is.null(conn)) {
    stop("Invalid database connection")
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
      if (debug) cat("[DEBUG] Executing query to get database locks\n")
      result <- DBI::dbGetQuery(conn, "
      SELECT
        l.locktype,
        l.database,
        l.relation,
        l.page,
        l.tuple,
        l.virtualxid,
        l.transactionid,
        l.classid,
        l.objid,
        l.objsubid,
        l.virtualtransaction,
        l.pid,
        l.mode,
        l.granted,
        l.fastpath
      FROM pg_locks l
      ORDER BY l.pid, l.locktype
    ")
      if (debug) cat("[DEBUG] Retrieved", nrow(result), "locks\n")
      return(result)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error monitoring locks:", e$message, "\n")
      stop(paste("Failed to monitor locks:", e$message))
    }
  )
}
