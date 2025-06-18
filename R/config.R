#' Configure TimescaleDB connection
#'
#' @param host Database host
#' @param port Database port
#' @param db Database name
#' @param user Database user
#' @param pass Database password
#' @param schema Schema name (optional)
#' @return List with connection parameters
#' @export
#' @examples
#' \dontrun{
#' config <- configure_timescale(
#'   host = "localhost",
#'   port = 5432,
#'   db = "testdb",
#'   user = "testuser",
#'   pass = "testpass"
#' )
#' }
configure_timescale <- function(host, port, db, user, pass, schema = "public") {
  # Validate host
  if (!is.character(host) || nchar(host) == 0) {
    stop("Invalid host")
  }

  # Validate port
  if (!is.numeric(port) || port <= 0 || port > 65535) {
    stop("Invalid port")
  }

  # Validate database name
  if (!is.character(db) || nchar(db) == 0) {
    stop("Invalid database name")
  }

  # Validate user
  if (!is.character(user) || nchar(user) == 0) {
    stop("Invalid user")
  }

  # Validate password
  if (!is.character(pass)) {
    stop("Invalid password")
  }

  # Validate schema
  if (!is.character(schema) || nchar(schema) == 0) {
    stop("Invalid schema")
  }

  # Return configuration
  list(
    host = host,
    port = port,
    db = db,
    user = user,
    pass = pass,
    schema = schema
  )
}

#' Connect to database
#'
#' @param config Connection configuration
#' @return Database connection object
#' @export
#' @examples
#' \dontrun{
#' conn <- connect_to_db(config)
#' }
connect_to_db <- function(config) {
  if (!is.list(config) || !all(c("host", "port", "db", "user", "pass") %in% names(config))) {
    stop("Invalid configuration")
  }

  tryCatch(
    {
      conn <- DBI::dbConnect(
        RPostgres::Postgres(),
        host = config$host,
        port = config$port,
        dbname = config$db,
        user = config$user,
        password = config$pass
      )

      # Set schema if specified
      if (!is.null(config$schema)) {
        DBI::dbExecute(conn, paste("SET search_path TO", config$schema))
      }

      conn
    },
    error = function(e) {
      stop(paste("Failed to connect to database:", e$message))
    }
  )
}

#' Disconnect from database
#'
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return TRUE if successful
#' @export
#' @examples
#' \dontrun{
#' # Using global connection
#' init_connection(dbname = "testdb", host = "localhost")
#' disconnect_from_db()
#' 
#' # Or with explicit connection
#' conn <- get_connection()
#' disconnect_from_db(conn)
#' }
disconnect_from_db <- function(conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Disconnecting from database\n")
  
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
      if (debug) cat("[DEBUG] Executing disconnect\n")
      DBI::dbDisconnect(conn)
      if (debug) cat("[DEBUG] Successfully disconnected from database\n")
      TRUE
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error disconnecting from database:", e$message, "\n")
      stop(paste("Failed to disconnect from database:", e$message))
    }
  )
}

#' Set schema
#'
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param schema Schema name
#' @param debug Enable debug messages (optional, default FALSE)
#' @return TRUE if successful
#' @export
#' @examples
#' \dontrun{
#' # Using global connection
#' init_connection(dbname = "testdb", host = "localhost")
#' set_schema("timeseries")
#' 
#' # Or with explicit connection
#' conn <- get_connection()
#' set_schema(conn, "timeseries")
#' }
set_schema <- function(conn = NULL, schema, debug = FALSE) {
  if (debug) cat("[DEBUG] Setting schema to:", schema, "\n")
  
  # Use global connection if not provided
  if (is.null(conn)) {
    if (debug) cat("[DEBUG] Using global connection\n")
    conn <- get_connection()
  }
  
  if (!DBI::dbIsValid(conn)) {
    stop("Invalid database connection")
  }

  if (!is.character(schema) || nchar(schema) == 0) {
    stop("Invalid schema")
  }

  tryCatch(
    {
      if (debug) cat("[DEBUG] Executing SET search_path command\n")
      DBI::dbExecute(conn, paste("SET search_path TO", schema))
      if (debug) cat("[DEBUG] Schema successfully set to:", schema, "\n")
      TRUE
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error setting schema:", e$message, "\n")
      stop(paste("Failed to set schema:", e$message))
    }
  )
}

#' Get search path
#'
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return Current search path
#' @export
#' @examples
#' \dontrun{
#' # Using global connection
#' init_connection(dbname = "testdb", host = "localhost")
#' path <- get_search_path()
#' 
#' # Or with explicit connection
#' conn <- get_connection()
#' path <- get_search_path(conn)
#' }
get_search_path <- function(conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Getting search path\n")
  
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
      if (debug) cat("[DEBUG] Executing SHOW search_path command\n")
      result <- DBI::dbGetQuery(conn, "SHOW search_path")$search_path
      if (debug) cat("[DEBUG] Current search path:", result, "\n")
      return(result)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error getting search path:", e$message, "\n")
      stop(paste("Failed to get search path:", e$message))
    }
  )
}

#' Debug connection
#'
#' @param config Connection configuration
#' @return List with connection information
#' @export
#' @examples
#' \dontrun{
#' debug_connection(config)
#' }
debug_connection <- function(config) {
  if (!is.list(config) || !all(c("host", "port", "db", "user", "pass") %in% names(config))) {
    stop("Invalid configuration")
  }

  tryCatch(
    {
      # Try to connect
      conn <- DBI::dbConnect(
        RPostgres::Postgres(),
        host = config$host,
        port = config$port,
        dbname = config$db,
        user = config$user,
        password = config$pass
      )

      # Get connection info
      info <- list(
        connected = TRUE,
        version = DBI::dbGetQuery(conn, "SELECT version()")$version,
        search_path = DBI::dbGetQuery(conn, "SHOW search_path")$search_path,
        timezone = DBI::dbGetQuery(conn, "SHOW timezone")$timezone
      )

      # Disconnect
      DBI::dbDisconnect(conn)

      info
    },
    error = function(e) {
      list(
        connected = FALSE,
        error = e$message
      )
    }
  )
}

#' Get connection count
#'
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return Number of active connections
#' @export
#' @examples
#' \dontrun{
#' # Using global connection
#' init_connection(dbname = "testdb", host = "localhost")
#' count <- connection_count()
#' 
#' # Or with explicit connection
#' conn <- get_connection()
#' count <- connection_count(conn)
#' }
connection_count <- function(conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Getting connection count\n")
  
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
      if (debug) cat("[DEBUG] Executing query to count active connections\n")
      result <- DBI::dbGetQuery(conn, "
        SELECT count(*) as count
        FROM pg_stat_activity
      ")
      if (debug) cat("[DEBUG] Active connections:", result$count, "\n")
      return(result$count)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error getting connection count:", e$message, "\n")
      stop(paste("Failed to get connection count:", e$message))
    }
  )
}

#' Initialize a global database connection
#'
#' Creates and stores a global connection to the TimescaleDB database that can be used
#' by other functions without explicitly passing a connection object. The connection
#' configuration is also stored to enable automatic reconnection if the connection is lost.
#'
#' @section Using with namespace qualification:
#' When using the TimescaleR package with namespace qualification (e.g., TimescaleR::function_name()),
#' be aware that you should either:
#' 
#' 1. Load the entire package with \code{library(TimescaleR)} before using functions
#' 2. Always use the namespace prefix for all functions: \code{TimescaleR::init_connection()},
#'    \code{TimescaleR::get_query()}, etc.
#'
#' @param host Database host
#' @param port Database port
#' @param db Database name (also accepts "dbname" for compatibility)
#' @param user Database user
#' @param pass Database password
#' @param schema Schema name (optional)
#' @param debug Enable debug messages (optional, default FALSE)
#' @param ... Additional parameters (for dbname compatibility)
#' @return TRUE if successful
#' @export
#' @examples
#' \dontrun{
#' # Initialize a global connection
#' init_connection(
#'   host = "localhost",
#'   port = 5432,
#'   db = "testdb", 
#'   user = "testuser",
#'   pass = "testpass"
#' )
#' 
#' # Alternative parameter format 
#' init_connection(dbname = "testdb", host = "localhost")
#' }
init_connection <- function(host, port = 5432, db = NULL, user = "postgres", pass = "",
                           schema = "public", debug = FALSE, ...) {
  # Support for dbname parameter for compatibility
  args <- list(...)
  if (is.null(db) && !is.null(args$dbname)) {
    db <- args$dbname
  }
  
  if (debug) cat("[DEBUG] Initializing global connection\n")
  
  # Validate parameters
  if (!is.character(host) || nchar(host) == 0) {
    stop("Invalid host")
  }
  
  if (!is.numeric(port) || port <= 0 || port > 65535) {
    stop("Invalid port")
  }
  
  if (is.null(db)) {
    stop("Database name is required. Provide either 'db' or 'dbname'")
  }
  
  if (!is.character(db) || nchar(db) == 0) {
    stop("Invalid database name")
  }
  
  if (!is.character(user) || nchar(user) == 0) {
    stop("Invalid user")
  }
  
  if (!is.character(pass)) {
    stop("Invalid password")
  }
  
  if (!is.character(schema) || nchar(schema) == 0) {
    stop("Invalid schema")
  }
  
  # Create configuration
  config <- list(
    host = host,
    port = port,
    db = db,
    user = user,
    pass = pass,
    schema = schema
  )
  
  # Access the package namespace directly
  ns <- asNamespace("TimescaleR")
  
  # Ensure environment exists
  if (!exists(".TimescaleR", envir = ns)) {
    stop("Package environment not initialized")
  }
  
  # Get environment
  env <- get(".TimescaleR", envir = ns)
  
  tryCatch(
    {
      if (debug) cat("[DEBUG] Connecting to database\n")
      
      # Connect to database
      conn <- connect_to_db(config)
      
      # Store connection and config in package environment
      env$global_conn <- conn
      env$connection_config <- config
      
      if (debug) cat("[DEBUG] Global connection initialized successfully\n")
      return(TRUE)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error initializing global connection:", e$message, "\n")
      stop(paste("Failed to initialize global connection:", e$message))
    }
  )
}

#' Get the global database connection
#'
#' Retrieves the global database connection initialized with init_connection().
#' If no global connection exists or the connection is no longer valid,
#' it will attempt to automatically reconnect using the stored configuration.
#'
#' @param auto_reconnect Whether to automatically attempt reconnection if needed (default: TRUE)
#' @param debug Enable verbose debug output
#' @return Database connection object
#' @export
#' @examples
#' \dontrun{
#' # Initialize global connection
#' init_connection(host = "localhost", port = 5432, db = "testdb", 
#'                 user = "testuser", pass = "testpass")
#'                 
#' # Later, get the connection
#' conn <- get_connection()
#' 
#' # Use connection
#' result <- DBI::dbGetQuery(conn, "SELECT * FROM my_table")
#' }
get_connection <- function(auto_reconnect = TRUE, debug = FALSE) {
  ensure_valid_connection(auto_reconnect, debug)
}

#' Close the global database connection
#'
#' Closes the global database connection initialized with init_connection()
#' and removes it from the package environment.
#'
#' @param clear_config Whether to also clear the stored connection configuration (default: FALSE)
#' @param debug Enable verbose debug output
#' @return TRUE if successful
#' @export
#' @examples
#' \dontrun{
#' # Initialize global connection
#' init_connection(host = "localhost", port = 5432, db = "testdb", 
#'                 user = "testuser", pass = "testpass")
#'                 
#' # Use connection through other functions
#' result <- get_query("SELECT * FROM my_table")
#' 
#' # Close connection when done
#' close_connection()
#' }
close_connection <- function(clear_config = FALSE, debug = FALSE) {
  # Direct access to namespace
  ns <- asNamespace("TimescaleR")
  
  if (!exists(".TimescaleR", envir = ns)) {
    if (debug) message("No package environment found, nothing to close")
    return(TRUE)  # Nothing to close
  }
  
  # Get environment reference
  env <- get(".TimescaleR", envir = ns)
  
  if (!exists("global_conn", envir = env) || 
      is.null(env$global_conn)) {
    if (debug) message("No global connection found, nothing to close")
    return(TRUE)  # Nothing to close
  }
  
  # Check connection validity with error handling
  if (debug) message("Checking connection validity")
  valid_conn <- tryCatch({
    DBI::dbIsValid(env$global_conn)
  }, error = function(e) {
    if (debug) message("Error checking connection validity: ", e$message)
    FALSE
  })
  
  if (valid_conn) {
    if (debug) message("Connection is valid, disconnecting")
    tryCatch({
      DBI::dbDisconnect(env$global_conn)
      if (debug) message("Successfully disconnected")
    }, error = function(e) {
      warning(paste("Failed to disconnect:", e$message))
      if (debug) message("Failed to disconnect: ", e$message)
    })
  } else if (debug) {
    message("Connection is not valid, no need to disconnect")
  }
  
  # Remove connection from environment
  if (debug) message("Clearing connection from environment")
  env$global_conn <- NULL
  
  # Optionally clear the configuration
  if (clear_config) {
    if (debug) message("Clearing connection configuration")
    env$connection_config <- NULL
  }
  
  return(TRUE)
}
