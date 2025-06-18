# Package environment to store global connection and other shared objects
# Initialized in .onLoad
NULL

# Initialize the package environment at load time
.onLoad <- function(libname, pkgname) {
  # Create a new environment in the package namespace
  ns <- asNamespace(pkgname)
  
  # Initialize the environment if it doesn't exist
  if (!exists(".TimescaleR", envir = ns)) {
    assign(".TimescaleR", new.env(parent = emptyenv()), envir = ns)
    
    # Initialize connection_config as NULL
    env <- get(".TimescaleR", envir = ns)
    env$connection_config <- NULL
  }
}

# Package startup message
.onAttach <- function(libname, pkgname) {
  packageStartupMessage("TimescaleR loaded successfully. Use init_connection() to set up a global database connection.")
}

# Package unload handler
.onUnload <- function(libpath) {
  # Close any open connections
  ns <- asNamespace("TimescaleR")
  if (exists(".TimescaleR", envir = ns)) {
    env <- get(".TimescaleR", envir = ns)
    
    if (exists("global_conn", envir = env) && !is.null(env$global_conn)) {
      # Check if connection is still valid
      valid_conn <- tryCatch({
        DBI::dbIsValid(env$global_conn)
      }, error = function(e) {
        FALSE
      })
      
      if (valid_conn) {
        tryCatch({
          DBI::dbDisconnect(env$global_conn)
        }, error = function(e) {
          warning("Could not disconnect from database on package unload")
        })
      }
      
      # Clear global connection
      env$global_conn <- NULL
    }
  }
}

#' Check if a global connection exists
#'
#' Checks if a global database connection has been initialized and is valid.
#'
#' @param silent Whether to suppress messages (default: TRUE)
#' @return TRUE if a valid global connection exists, FALSE otherwise
#' @export
#' @examples
#' \dontrun{
#' # Check if a global connection exists
#' if (has_connection()) {
#'   # Use existing connection
#'   results <- get_query("SELECT * FROM my_table")
#' } else {
#'   # Initialize a new connection
#'   init_connection(host = "localhost", port = 5432, db = "testdb", 
#'                   user = "testuser", pass = "testpass")
#' }
#' }
has_connection <- function(silent = TRUE) {
  # Direct access to namespace
  ns <- asNamespace("TimescaleR")
  
  # Ensure environment exists
  if (!exists(".TimescaleR", envir = ns)) {
    if (!silent) message("Package environment not initialized.")
    return(FALSE)
  }
  
  # Get environment
  env <- get(".TimescaleR", envir = ns)
  
  # Check if global connection exists
  if (!exists("global_conn", envir = env) || 
      is.null(env$global_conn)) {
    if (!silent) message("No global connection exists.")
    return(FALSE)
  }
  
  # Check if connection is valid
  valid_conn <- tryCatch({
    DBI::dbIsValid(env$global_conn)
  }, error = function(e) {
    FALSE
  })
  
  if (!valid_conn && !silent) {
    message("Global connection exists but is no longer valid.")
  }
  
  return(valid_conn)
}

# Function to check if connection is valid and attempt reconnection if needed
ensure_valid_connection <- function(auto_reconnect = TRUE, debug = FALSE) {
  # Direct access to namespace
  ns <- asNamespace("TimescaleR")
  
  # Ensure environment exists
  if (!exists(".TimescaleR", envir = ns)) {
    if (debug) message("Package environment not initialized")
    stop("Package environment not initialized. This may be caused by a package loading issue.")
  }
  
  # Get environment
  env <- get(".TimescaleR", envir = ns)
  
  # Check if global connection exists
  if (!exists("global_conn", envir = env) || 
      is.null(env$global_conn)) {
    if (debug) message("No global connection found")
    if (!auto_reconnect || !exists("connection_config", envir = env) || 
        is.null(env$connection_config)) {
      stop("No global connection initialized. Call init_connection() first.")
    } else {
      # Attempt to reconnect using stored configuration
      message("Connection not found. Attempting to reconnect automatically...")
      config <- env$connection_config
      
      tryCatch({
        conn <- connect_to_db(config)
        env$global_conn <- conn
        message("Successfully reconnected to the database.")
      }, error = function(e) {
        stop(paste("Failed to reconnect to database:", e$message))
      })
      
      return(env$global_conn)
    }
  }
  
  # Check if connection is valid
  if (debug) message("Checking if connection is valid")
  valid_conn <- tryCatch({
    DBI::dbIsValid(env$global_conn)
  }, error = function(e) {
    if (debug) message("Error checking validity: ", e$message)
    FALSE
  })
  
  # If connection is invalid but we have config and auto_reconnect is TRUE
  if (!valid_conn && auto_reconnect && 
      exists("connection_config", envir = env) && 
      !is.null(env$connection_config)) {
    
    message("Connection lost. Attempting to reconnect automatically...")
    config <- env$connection_config
    
    tryCatch({
      # Close the invalid connection just in case
      tryCatch({
        DBI::dbDisconnect(env$global_conn)
      }, error = function(e) {
        # Ignore errors when disconnecting invalid connection
      })
      
      # Create new connection
      conn <- connect_to_db(config)
      env$global_conn <- conn
      message("Successfully reconnected to the database.")
    }, error = function(e) {
      stop(paste("Failed to reconnect to database:", e$message))
    })
  } else if (!valid_conn) {
    stop("Database connection is no longer valid. Please reinitialize with init_connection().")
  }
  
  if (debug) message("Connection is valid, returning")
  return(env$global_conn)
} 
