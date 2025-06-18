#' Backup a hypertable to a directory
#'
#' Creates a backup of a hypertable by exporting its data and structure to CSV files.
#' The backup includes both the table data and its structure definition.
#'
#' @param table_name Name of the hypertable to backup
#' @param backup_path Directory path where backup will be stored
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return TRUE if backup was successful
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
#' # Using global connection with required arguments first
#' backup_hypertable("my_table", "backup/path")
#' 
#' # Or with explicit connection as optional parameter
#' conn <- get_connection()
#' backup_hypertable("my_table", "backup/path", conn)
#' 
#' # With debugging enabled
#' backup_hypertable("my_table", "backup/path", debug = TRUE)
#' }
backup_hypertable <- function(table_name, backup_path, conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Backing up hypertable:", table_name, "to path:", backup_path, "\n")
  
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
      # Create backup directory if it doesn't exist
      if (!dir.exists(backup_path)) {
        if (debug) cat("[DEBUG] Creating backup directory:", backup_path, "\n")
        dir.create(backup_path, recursive = TRUE)
      }

      # Get table data
      if (debug) cat("[DEBUG] Retrieving data from table:", table_name, "\n")
      data <- DBI::dbGetQuery(conn, paste("SELECT * FROM", table_name))
      if (debug) cat("[DEBUG] Retrieved", nrow(data), "rows\n")

      # Convert to data.table
      if (debug) cat("[DEBUG] Converting to data.table\n")
      data <- data.table::as.data.table(data)

      # Write to file using fwrite
      backup_file <- file.path(backup_path, paste0(table_name, ".csv"))
      if (debug) cat("[DEBUG] Writing data to file:", backup_file, "\n")
      data.table::fwrite(data, backup_file)
      if (debug) cat("[DEBUG] Data written successfully\n")

      # Get and save table structure
      if (debug) cat("[DEBUG] Getting table structure\n")
      structure <- get_table_structure(conn, table_name)
      structure <- data.table::as.data.table(structure)
      structure_file <- file.path(backup_path, paste0(table_name, "_structure.csv"))
      if (debug) cat("[DEBUG] Writing structure to file:", structure_file, "\n")
      data.table::fwrite(structure, structure_file)
      if (debug) cat("[DEBUG] Structure written successfully\n")

      if (debug) cat("[DEBUG] Backup completed successfully\n")
      return(TRUE)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error backing up hypertable:", e$message, "\n")
      stop(paste("Failed to backup hypertable:", e$message))
    }
  )
}

#' Restore a hypertable from a backup directory
#'
#' Restores a hypertable from backup files. This function expects both the data
#' and structure files to be present in the backup directory.
#'
#' @param table_name Name of the hypertable to restore
#' @param backup_path Directory path containing the backup
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return TRUE if restore was successful
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
#' # Using global connection with required arguments first
#' restore_hypertable("my_table", "backup/path")
#' 
#' # Or with explicit connection as optional parameter
#' conn <- get_connection()
#' restore_hypertable("my_table", "backup/path", conn)
#' 
#' # With debugging enabled
#' restore_hypertable("my_table", "backup/path", debug = TRUE)
#' }
restore_hypertable <- function(table_name, backup_path, conn = NULL, debug = FALSE) {
  if (debug) cat("[DEBUG] Restoring hypertable:", table_name, "from path:", backup_path, "\n")
  
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
      # Check if backup files exist
      data_file <- file.path(backup_path, paste0(table_name, ".csv"))
      structure_file <- file.path(backup_path, paste0(table_name, "_structure.csv"))

      if (debug) {
        cat("[DEBUG] Looking for data file:", data_file, "\n")
        cat("[DEBUG] Looking for structure file:", structure_file, "\n")
      }

      if (!file.exists(data_file) || !file.exists(structure_file)) {
        if (debug) cat("[DEBUG] Error: Backup files not found\n")
        stop("Backup files not found")
      }

      # Read data using fread
      if (debug) cat("[DEBUG] Reading data from file:", data_file, "\n")
      data <- data.table::fread(data_file)
      if (debug) cat("[DEBUG] Read", nrow(data), "rows\n")
      
      if (debug) cat("[DEBUG] Reading structure from file:", structure_file, "\n")
      structure <- data.table::fread(structure_file)

      # Create table if it doesn't exist
      if (debug) cat("[DEBUG] Checking if table exists:", table_name, "\n")
      if (!table_exists(conn, table_name)) {
        if (debug) cat("[DEBUG] Table does not exist, creating it\n")
        create_table_from_data_table(conn, data, table_name)
      } else {
        if (debug) cat("[DEBUG] Table already exists\n")
      }

      # Insert data
      if (debug) cat("[DEBUG] Inserting data into table:", table_name, "\n")
      DBI::dbWriteTable(conn, table_name, data, append = TRUE)
      if (debug) cat("[DEBUG] Data inserted successfully\n")

      if (debug) cat("[DEBUG] Restore completed successfully\n")
      return(TRUE)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error restoring hypertable:", e$message, "\n")
      stop(paste("Failed to restore hypertable:", e$message))
    }
  )
}
