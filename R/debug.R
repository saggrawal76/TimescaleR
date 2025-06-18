#' Debug environment state
#' 
#' Prints detailed information about the package environment state
#' to help diagnose connection issues.
#' 
#' @param verbose Whether to show detailed connection information (default: FALSE)
#' @return Invisibly returns a list with debug information
#' @export
#' @examples
#' \dontrun{
#' # Check environment state
#' debug_environment()
#' 
#' # With verbose output
#' debug_environment(verbose = TRUE)
#' }
debug_environment <- function(verbose = FALSE) {
  # Create results list
  debug_info <- list(
    timestamp = Sys.time(),
    r_version = R.version.string,
    package_version = utils::packageVersion("TimescaleR"),
    environment_exists = FALSE,
    global_conn_exists = FALSE,
    config_exists = FALSE,
    conn_valid = FALSE,
    pid = Sys.getpid()
  )
  
  # Check if environment exists
  cat("Checking TimescaleR environment state:\n")
  if (exists(".TimescaleR", envir = asNamespace("TimescaleR"))) {
    debug_info$environment_exists <- TRUE
    cat(" - Package environment (.TimescaleR): EXISTS\n")
    
    # Get environment reference
    env <- get(".TimescaleR", envir = asNamespace("TimescaleR"))
    
    # Check global_conn
    if (exists("global_conn", envir = env) && !is.null(env$global_conn)) {
      debug_info$global_conn_exists <- TRUE
      cat(" - Global connection object: EXISTS\n")
      
      # Check if connection is valid
      conn_valid <- tryCatch({
        DBI::dbIsValid(env$global_conn)
      }, error = function(e) {
        cat(" - Error checking connection validity:", e$message, "\n")
        FALSE
      })
      
      debug_info$conn_valid <- conn_valid
      if (conn_valid) {
        cat(" - Connection is valid: YES\n")
        
        if (verbose) {
          # Try to get connection info
          tryCatch({
            info <- DBI::dbGetInfo(env$global_conn)
            cat(" - Connection info:\n")
            for (name in names(info)) {
              cat("   * ", name, ": ", info[[name]], "\n", sep = "")
            }
            debug_info$connection_info <- info
          }, error = function(e) {
            cat(" - Error getting connection info:", e$message, "\n")
          })
        }
      } else {
        cat(" - Connection is valid: NO\n")
      }
    } else {
      cat(" - Global connection object: MISSING\n")
    }
    
    # Check connection config
    if (exists("connection_config", envir = env) && !is.null(env$connection_config)) {
      debug_info$config_exists <- TRUE
      cat(" - Connection configuration: EXISTS\n")
      
      if (verbose) {
        # Show sanitized config (hide password)
        config <- env$connection_config
        safe_config <- config
        if ("pass" %in% names(safe_config)) {
          safe_config$pass <- "********"
        }
        cat(" - Connection config:\n")
        for (name in names(safe_config)) {
          cat("   * ", name, ": ", safe_config[[name]], "\n", sep = "")
        }
        debug_info$connection_config <- safe_config
      }
    } else {
      cat(" - Connection configuration: MISSING\n")
    }
    
    # List all objects in environment
    if (verbose) {
      cat(" - All objects in environment:\n")
      obj_names <- ls(envir = env)
      for (name in obj_names) {
        obj <- env[[name]]
        cat("   * ", name, " (", class(obj)[1], ")\n", sep = "")
      }
      debug_info$environment_objects <- obj_names
    }
  } else {
    cat(" - Package environment (.TimescaleR): MISSING\n")
    cat(" - This is likely the root cause of connection issues.\n")
  }
  
  # Check for other potential issues
  cat("\nTroubleshooting tips:\n")
  cat(" 1. Use library(TimescaleR) before making any function calls\n")
  cat(" 2. Always use TimescaleR::init_connection() first\n")
  cat(" 3. For namespace calls (TimescaleR::function), make sure all calls use the namespace\n")
  
  invisible(debug_info)
}

#' Check and fix global connection
#' 
#' Attempts to check and fix the global connection state.
#' This is a maintenance function for troubleshooting.
#' 
#' @param config Connection configuration to use if reconnection is needed
#' @return TRUE if connection is valid or was successfully fixed
#' @export
#' @examples
#' \dontrun{
#' # Simple check
#' fix_connection()
#' 
#' # With explicit config
#' config <- list(
#'   host = "localhost",
#'   port = 5432,
#'   db = "testdb",
#'   user = "testuser",
#'   pass = "testpass",
#'   schema = "public"
#' )
#' fix_connection(config)
#' }
fix_connection <- function(config = NULL) {
  cat("Checking global connection status...\n")
  
  # Get namespace environment
  ns <- asNamespace("TimescaleR")
  
  # Check if environment exists
  if (!exists(".TimescaleR", envir = ns)) {
    cat("Creating missing package environment...\n")
    assign(".TimescaleR", new.env(parent = emptyenv()), envir = ns)
  }
  
  # Get environment reference
  env <- get(".TimescaleR", envir = ns)
  
  # Check if connection exists and is valid
  conn_exists <- exists("global_conn", envir = env) && !is.null(env$global_conn)
  
  if (conn_exists) {
    conn_valid <- tryCatch({
      DBI::dbIsValid(env$global_conn)
    }, error = function(e) {
      cat("Error checking connection validity:", e$message, "\n")
      FALSE
    })
    
    if (conn_valid) {
      cat("Global connection exists and is valid. No fix needed.\n")
      return(TRUE)
    } else {
      cat("Global connection exists but is invalid. Attempting to fix...\n")
    }
  } else {
    cat("Global connection does not exist. Attempting to create...\n")
  }
  
  # Get configuration
  if (is.null(config)) {
    if (exists("connection_config", envir = env) && !is.null(env$connection_config)) {
      config <- env$connection_config
      cat("Using stored connection configuration.\n")
    } else {
      cat("No connection configuration available. Cannot fix connection.\n")
      cat("Please provide a configuration or call init_connection() with parameters.\n")
      return(FALSE)
    }
  }
  
  # Try to connect
  tryCatch({
    cat("Connecting to database...\n")
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
      cat("Setting schema to", config$schema, "...\n")
      DBI::dbExecute(conn, paste("SET search_path TO", config$schema))
    }
    
    # Store in environment
    env$global_conn <- conn
    env$connection_config <- config
    
    cat("Connection fixed successfully.\n")
    return(TRUE)
  }, error = function(e) {
    cat("Failed to fix connection:", e$message, "\n")
    return(FALSE)
  })
} 
