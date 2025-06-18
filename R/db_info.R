#' Get chunks information for a hypertable
#'
#' Retrieves information about chunks in a TimescaleDB hypertable,
#' including chunk size, compression status, and time ranges.
#'
#' @param table_name Name of the hypertable
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return Data frame with chunk information
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
#' # Get chunks info
#' chunks <- get_chunks_info("sensor_data")
#' print(chunks)
#' 
#' # Or with explicit connection
#' conn <- get_connection()
#' chunks <- get_chunks_info("sensor_data", conn)
#' }
get_chunks_info <- function(table_name, conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Getting chunks information for table:", table_name, "\n")
  
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
      if (debug) cat("[DEBUG] Executing query to get chunks information\n")
      result <- DBI::dbGetQuery(conn, "
      SELECT 
        chunk_schema,
        chunk_name,
        range_start,
        range_end,
        is_compressed,
        chunk_table_size,
        compressed_chunk_size,
        uncompressed_heap_size,
        uncompressed_toast_size,
        uncompressed_index_size
      FROM timescaledb_information.chunks
      WHERE hypertable_name = $1
      ORDER BY range_start
    ", params = list(table_name))
      
      if (debug) cat("[DEBUG] Retrieved information for", nrow(result), "chunks\n")
      return(result)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error getting chunks information:", e$message, "\n")
      stop(paste("Failed to get chunks information:", e$message))
    }
  )
}

#' Show policies for a hypertable
#'
#' Displays all policies (compression, retention, refresh) configured for a hypertable.
#'
#' @param table_name Name of the hypertable (optional, shows all if NULL)
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return Data frame with policy information
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
#' # Show all policies
#' policies <- show_policies()
#' 
#' # Show policies for specific table
#' policies <- show_policies("sensor_data")
#' 
#' # Or with explicit connection
#' conn <- get_connection()
#' policies <- show_policies("sensor_data", conn)
#' }
show_policies <- function(table_name = NULL, conn = NULL, debug = FALSE) {
  if (debug) {
    if (is.null(table_name)) {
      cat("[DEBUG] Showing all policies\n")
    } else {
      cat("[DEBUG] Showing policies for table:", table_name, "\n")
    }
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
      # Build query based on whether table_name is specified
      if (is.null(table_name)) {
        if (debug) cat("[DEBUG] Executing query to show all policies\n")
        query <- "
        SELECT 
          'compression' as policy_type,
          hypertable_name,
          compress_after as config
        FROM timescaledb_information.compression_settings
        UNION ALL
        SELECT 
          'retention' as policy_type,
          hypertable_name,
          drop_after as config
        FROM timescaledb_information.drop_chunks_policies
        UNION ALL
        SELECT 
          'refresh' as policy_type,
          view_name as hypertable_name,
          schedule_interval as config
        FROM timescaledb_information.continuous_aggregate_policies
        ORDER BY hypertable_name, policy_type
        "
        result <- DBI::dbGetQuery(conn, query)
      } else {
        if (debug) cat("[DEBUG] Executing query to show policies for specific table\n")
        query <- "
        SELECT 
          'compression' as policy_type,
          hypertable_name,
          compress_after as config
        FROM timescaledb_information.compression_settings
        WHERE hypertable_name = $1
        UNION ALL
        SELECT 
          'retention' as policy_type,
          hypertable_name,
          drop_after as config
        FROM timescaledb_information.drop_chunks_policies
        WHERE hypertable_name = $1
        UNION ALL
        SELECT 
          'refresh' as policy_type,
          view_name as hypertable_name,
          schedule_interval as config
        FROM timescaledb_information.continuous_aggregate_policies
        WHERE view_name = $1
        ORDER BY policy_type
        "
        result <- DBI::dbGetQuery(conn, query, params = list(table_name))
      }
      
      if (debug) cat("[DEBUG] Retrieved", nrow(result), "policies\n")
      return(result)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error showing policies:", e$message, "\n")
      stop(paste("Failed to show policies:", e$message))
    }
  )
}

#' Compress chunks for a hypertable
#'
#' Manually compress chunks for a hypertable that are older than the specified time.
#'
#' @param table_name Name of the hypertable
#' @param older_than Time interval (e.g., "7 days", "1 month")
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return Number of chunks compressed
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
#' # Compress chunks older than 7 days
#' compressed <- compress_chunks("sensor_data", "7 days")
#' 
#' # Or with explicit connection
#' conn <- get_connection()
#' compressed <- compress_chunks("sensor_data", "7 days", conn)
#' }
compress_chunks <- function(table_name, older_than, conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Compressing chunks for table:", table_name, "older than:", older_than, "\n")
  
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
      if (debug) cat("[DEBUG] Executing compress_chunk command\n")
      result <- DBI::dbGetQuery(conn, "
      SELECT compress_chunk(chunk_name)
      FROM timescaledb_information.chunks
      WHERE hypertable_name = $1
      AND range_end < NOW() - INTERVAL $2
      AND NOT is_compressed
    ", params = list(table_name, older_than))
      
      num_compressed <- nrow(result)
      if (debug) cat("[DEBUG] Compressed", num_compressed, "chunks\n")
      return(num_compressed)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error compressing chunks:", e$message, "\n")
      stop(paste("Failed to compress chunks:", e$message))
    }
  )
}


