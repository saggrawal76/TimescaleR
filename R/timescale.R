#' Check if TimescaleDB is supported
#'
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return TRUE if TimescaleDB is supported
#' @export
#'
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
#' # Using global connection
#' check_timescaledb_support()
#' 
#' # With explicit connection
#' conn <- get_connection()
#' check_timescaledb_support(conn)
#' }
check_timescaledb_support <- function(conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Checking TimescaleDB support\n")
  
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
      if (debug) cat("[DEBUG] Executing query to check TimescaleDB extension\n")
      result <- DBI::dbGetQuery(conn, "SELECT default_version, installed_version FROM pg_available_extensions WHERE name = 'timescaledb'")
      is_supported <- nrow(result) > 0 && !is.na(result$installed_version)
      if (debug) cat("[DEBUG] TimescaleDB supported:", is_supported, "\n")
      return(is_supported)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error checking TimescaleDB support:", e$message, "\n")
      stop(paste("Failed to check TimescaleDB support:", e$message))
    }
  )
}

#' Check if a table is a hypertable
#'
#' @param table_name Name of the table
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return TRUE if the table is a hypertable
#' @export
#'
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
#' # Using global connection with required arguments first
#' is_hypertable("sensor_data")
#' 
#' # With explicit connection as optional parameter
#' conn <- get_connection()
#' is_hypertable("sensor_data", conn)
#' }
is_hypertable <- function(table_name, conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Checking if table is a hypertable:", table_name, "\n")
  
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
      if (debug) cat("[DEBUG] Executing query to check if table is a hypertable\n")
      query <- "
      SELECT EXISTS (
        SELECT FROM _timescaledb_catalog.hypertable h
        WHERE h.table_name = $1
        AND h.schema_name = current_schema()
      )"
      
      result <- DBI::dbGetQuery(conn, query, params = list(table_name))
      
      is_hyper <- result$exists[1]
      if (debug) {
        if (is_hyper) {
          cat("[DEBUG] Table", table_name, "is a hypertable\n")
        } else {
          cat("[DEBUG] Table", table_name, "is not a hypertable\n")
        }
      }
      
      return(is_hyper)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error checking if table is a hypertable:", e$message, "\n")
      stop(paste("Failed to check if table is hypertable:", e$message))
    }
  )
}

#' Add compression policy to a hypertable
#'
#' @param table_name Name of the hypertable
#' @param compress_after Time interval after which to compress chunks
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return TRUE if successful
#' @export
#'
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
#' # Using global connection with required arguments first
#' add_compression_policy("sensor_data", "7 days")
#' 
#' # With explicit connection as optional parameter
#' conn <- get_connection()
#' add_compression_policy("sensor_data", "7 days", conn)
#' }
add_compression_policy <- function(table_name, compress_after, conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Adding compression policy to hypertable:", table_name, "\n")
  
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
      if (debug) cat("[DEBUG] Executing query to add compression policy\n")
      DBI::dbExecute(conn, "
      SELECT add_compression_policy($1, INTERVAL $2)
    ", params = list(table_name, compress_after))
      if (debug) cat("[DEBUG] Compression policy added successfully\n")
      return(TRUE)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error adding compression policy:", e$message, "\n")
      stop(paste("Failed to add compression policy:", e$message))
    }
  )
}

#' Add retention policy to a hypertable
#'
#' @param table_name Name of the hypertable
#' @param drop_after Time interval after which to drop chunks
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return TRUE if successful
#' @export
#'
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
#' # Using global connection with required arguments first
#' add_retention_policy("sensor_data", "30 days")
#' 
#' # With explicit connection as optional parameter
#' conn <- get_connection()
#' add_retention_policy("sensor_data", "30 days", conn)
#' }
add_retention_policy <- function(table_name, drop_after, conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Adding retention policy to hypertable:", table_name, "\n")
  
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
      if (debug) cat("[DEBUG] Executing query to add retention policy\n")
      DBI::dbExecute(conn, "
      SELECT add_retention_policy($1, INTERVAL $2)
    ", params = list(table_name, drop_after))
      if (debug) cat("[DEBUG] Retention policy added successfully\n")
      return(TRUE)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error adding retention policy:", e$message, "\n")
      stop(paste("Failed to add retention policy:", e$message))
    }
  )
}

#' Get information about chunks in a hypertable
#'
#' @param table_name Name of the hypertable
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return Data frame with chunk information
#' @export
#'
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
#' # Get chunks info with required arguments first
#' chunks_info <- get_chunks_info("sensor_data")
#' print(chunks_info)
#' 
#' # With explicit connection as optional parameter
#' conn <- get_connection()
#' chunks_info <- get_chunks_info("sensor_data", conn)
#' }
get_chunks_info <- function(table_name, conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Getting chunks info for hypertable:", table_name, "\n")
  
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
      if (debug) cat("[DEBUG] Executing query to get chunks info\n")
      result <- DBI::dbGetQuery(conn, "
      SELECT
        chunk_name,
        range_start,
        range_end,
        is_compressed,
        chunk_size,
        table_size,
        index_size
      FROM timescaledb_information.chunks
      WHERE hypertable_name = $1
      ORDER BY range_start
    ", params = list(table_name))
      if (debug) cat("[DEBUG] Retrieved", nrow(result), "chunks\n")
      return(result)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error getting chunks info:", e$message, "\n")
      stop(paste("Failed to get chunks info:", e$message))
    }
  )
}

#' Show policies for a hypertable
#'
#' @param table_name Name of the hypertable
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return Data frame with policy information
#' @export
#'
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
#' # Using global connection with required arguments first
#' policies <- show_policies("sensor_data")
#' 
#' # With explicit connection as optional parameter
#' conn <- get_connection()
#' policies <- show_policies("sensor_data", conn)
#' }
show_policies <- function(table_name, conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Showing policies for hypertable:", table_name, "\n")
  
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
      if (debug) cat("[DEBUG] Executing query to show policies\n")
      result <- DBI::dbGetQuery(conn, "
      SELECT
        policy_name,
        hypertable_name,
        schedule_interval,
        job_type,
        config
      FROM timescaledb_information.jobs
      WHERE hypertable_name = $1
    ", params = list(table_name))
      if (debug) cat("[DEBUG] Retrieved", nrow(result), "policies\n")
      return(result)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error showing policies:", e$message, "\n")
      stop(paste("Failed to show policies:", e$message))
    }
  )
}

#' Compress chunks in a hypertable
#'
#' @param table_name Name of the hypertable
#' @param older_than Time interval for chunks to compress
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return TRUE if successful
#' @export
#'
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
#' # Using global connection with required arguments first
#' compress_chunks("sensor_data", "30 days")
#' 
#' # With explicit connection as optional parameter
#' conn <- get_connection()
#' compress_chunks("sensor_data", "30 days", conn)
#' }
compress_chunks <- function(table_name, older_than, conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Compressing chunks for hypertable:", table_name, "\n")
  
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
      if (debug) cat("[DEBUG] Executing query to compress chunks\n")
      DBI::dbExecute(conn, "
      SELECT compress_chunks($1, INTERVAL $2)
    ", params = list(table_name, older_than))
      if (debug) cat("[DEBUG] Chunks compressed successfully\n")
      return(TRUE)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error compressing chunks:", e$message, "\n")
      stop(paste("Failed to compress chunks:", e$message))
    }
  )
}

#' Decompress chunks in a hypertable
#'
#' @param table_name Name of the hypertable
#' @param older_than Time interval for chunks to decompress
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return TRUE if successful
#' @export
#'
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
#' # Using global connection with required arguments first
#' decompress_chunks("sensor_data", "7 days")
#' 
#' # With explicit connection as optional parameter
#' conn <- get_connection()
#' decompress_chunks("sensor_data", "7 days", conn)
#' }
decompress_chunks <- function(table_name, older_than, conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Decompressing chunks for hypertable:", table_name, "\n")
  
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
      if (debug) cat("[DEBUG] Executing query to decompress chunks\n")
      DBI::dbExecute(conn, "
      SELECT decompress_chunks($1, INTERVAL $2)
    ", params = list(table_name, older_than))
      if (debug) cat("[DEBUG] Chunks decompressed successfully\n")
      return(TRUE)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error decompressing chunks:", e$message, "\n")
      stop(paste("Failed to decompress chunks:", e$message))
    }
  )
}

#' Check if a Table is a Hypertable
#'
#' Determines whether a given table is a TimescaleDB hypertable.
#'
#' @param table_name Name of the table to check
#' @param conn Database connection object (optional if global connection is initialized)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return TRUE if the table is a hypertable, FALSE otherwise
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
#' # Check if a table is a hypertable with required arguments first
#' is_hyper <- is_hypertable("my_table")
#' if (is_hyper) {
#'   cat("Table is a hypertable\n")
#' } else {
#'   cat("Table is a regular table\n")
#' }
#' 
#' # With explicit connection
#' conn <- get_connection()
#' is_hyper <- is_hypertable("my_table", conn)
#' 
#' close_connection()
#' }
is_hypertable <- function(table_name, conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Checking if table is a hypertable:", table_name, "\n")
  
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
      if (debug) cat("[DEBUG] Executing query to check if table is a hypertable\n")
      query <- "
      SELECT EXISTS (
        SELECT FROM _timescaledb_catalog.hypertable h
        WHERE h.table_name = $1
        AND h.schema_name = current_schema()
      )"
      
      result <- DBI::dbGetQuery(conn, query, params = list(table_name))
      
      is_hyper <- result$exists[1]
      if (debug) {
        if (is_hyper) {
          cat("[DEBUG] Table", table_name, "is a hypertable\n")
        } else {
          cat("[DEBUG] Table", table_name, "is not a hypertable\n")
        }
      }
      
      return(is_hyper)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error checking if table is a hypertable:", e$message, "\n")
      stop(paste("Failed to check if table is hypertable:", e$message))
    }
  )
}

#' Add Compression Policy to Hypertable
#'
#' Sets up automatic compression for chunks older than a specified interval.
#'
#' @param table_name Name of the hypertable
#' @param compress_after Time interval after which to compress chunks (e.g., "7 days")
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
#' # Add a compression policy with required arguments first
#' add_compression_policy("my_table", "7 days")
#' 
#' # With explicit connection
#' conn <- get_connection()
#' add_compression_policy("my_table", "30 days", conn)
#' 
#' close_connection()
#' }
add_compression_policy <- function(table_name, compress_after, conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Adding compression policy to hypertable:", table_name, "\n")
  
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
      if (debug) cat("[DEBUG] Executing query to add compression policy\n")
      DBI::dbExecute(conn, "
      SELECT add_compression_policy($1, INTERVAL $2)
    ", params = list(table_name, compress_after))
      if (debug) cat("[DEBUG] Compression policy added successfully\n")
      return(TRUE)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error adding compression policy:", e$message, "\n")
      stop(paste("Failed to add compression policy:", e$message))
    }
  )
}

#' Add Retention Policy to Hypertable
#'
#' Sets up automatic data retention by dropping chunks older than a specified interval.
#'
#' @param table_name Name of the hypertable
#' @param drop_after Time interval after which to drop chunks (e.g., "90 days")
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
#' # Add a retention policy with required arguments first
#' add_retention_policy("my_table", "90 days")
#' 
#' # With explicit connection
#' conn <- get_connection()
#' add_retention_policy("my_table", "180 days", conn)
#' 
#' close_connection()
#' }
add_retention_policy <- function(table_name, drop_after, conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Adding retention policy to hypertable:", table_name, "\n")
  
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
      if (debug) cat("[DEBUG] Executing query to add retention policy\n")
      DBI::dbExecute(conn, "
      SELECT add_retention_policy($1, INTERVAL $2)
    ", params = list(table_name, drop_after))
      if (debug) cat("[DEBUG] Retention policy added successfully\n")
      return(TRUE)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error adding retention policy:", e$message, "\n")
      stop(paste("Failed to add retention policy:", e$message))
    }
  )
}
