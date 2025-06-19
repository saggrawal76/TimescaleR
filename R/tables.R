#' Create Table from Data Table
#'
#' Creates a new table in TimescaleDB from a data.table object.
#' Optionally converts it to a hypertable.
#'
#' @param data_table data.table object to create table from
#' @param table_name Name of the table to create
#' @param conn Database connection object (optional if global connection is initialized)
#' @param primary_keys Vector of column names to use as primary keys
#' @param date_columns Vector of column names to convert to DATE type
#' @param date_time_columns Vector of column names to convert to TIMESTAMP type
#' @param as_hypertable Logical, whether to convert to hypertable
#' @param time_column Name of the time column (required if as_hypertable is TRUE)
#' @param schema Schema name (optional)
#' @param chunk_time_interval Interval for chunking (e.g., "1 day")
#' @param if_not_exists Logical, whether to skip if table exists
#' @param index_columns Columns to index
#' @param primary_key Primary key column(s)
#' @param clean_nulls Logical, whether to remove or replace NULL values in primary keys (default: TRUE)
#' @param null_replacement Value to replace NULLs with in primary key columns if clean_nulls is TRUE (default: "UNKNOWN")
#' @param debug Enable debug messages (optional, default FALSE)
#' @return TRUE if successful
#' @export
#' @examples
#' \dontrun{
#' # Create a sample data.table
#' data <- data.table::data.table(
#'   time = seq(as.POSIXct("2023-01-01"), by = "1 hour", length.out = 24),
#'   value = rnorm(24),
#'   category = rep(c("A", "B"), each = 12)
#' )
#'
#' # Using global connection with required arguments first
#' init_connection(config)
#' create_table_from_data_table(data, "my_table", as_hypertable = TRUE, time_column = "time")
#' close_connection()
#'
#' # With explicit connection as optional parameter
#' conn <- connect_to_db(config)
#' create_table_from_data_table(data, "my_table", conn, as_hypertable = TRUE, time_column = "time")
#' disconnect_from_db(conn)
#' }
create_table_from_data_table <- function(data_table, table_name, conn = NULL,
                                         primary_keys = NULL,
                                         date_columns = NULL,
                                         date_time_columns = NULL,
                                         as_hypertable = FALSE,
                                         time_column = NULL,
                                         schema = NULL,
                                         chunk_time_interval = "1 day",
                                         if_not_exists = FALSE,
                                         index_columns = NULL,
                                         primary_key = NULL,
                                         clean_nulls = TRUE,
                                         null_replacement = "UNKNOWN",
                                         debug = FALSE) {
  if (debug) cat("[DEBUG] Creating table from data.table:", table_name, "\n")
  
  # Get connection if not provided
  if (is.null(conn)) {
    if (debug) cat("[DEBUG] Using global connection\n")
    conn <- get_connection()
  }
  
  # Validate connection object
  if (!inherits(conn, "DBIConnection")) {
    if (debug) cat("[DEBUG] Error: Invalid connection object\n")
    stop("Invalid connection object: must be a DBIConnection object")
  }

  if (!DBI::dbIsValid(conn)) {
    if (debug) cat("[DEBUG] Error: Invalid database connection\n")
    stop("Invalid database connection")
  }

  if (!data.table::is.data.table(data_table)) {
    if (debug) cat("[DEBUG] Error: Input is not a data.table\n")
    stop("data_table must be a data.table")
  }

  if (as_hypertable && is.null(time_column)) {
    if (debug) cat("[DEBUG] Error: time_column missing for hypertable conversion\n")
    stop("time_column must be specified when as_hypertable is TRUE")
  }

  tryCatch(
    {
      # Handle NULL values in primary key columns if required
      if (!is.null(primary_keys) && clean_nulls) {
        if (debug) cat("[DEBUG] Checking for NULL values in primary key columns\n")
        for (key_col in primary_keys) {
          if (key_col %in% names(data_table)) {
            # Find NULL or NA values
            null_indices <- which(is.na(data_table[[key_col]]))
            if (length(null_indices) > 0) {
              if (debug) cat("[DEBUG] Found", length(null_indices), "NULL values in column", key_col, "\n")
              # Replace NULL values with the replacement value
              data_table[null_indices, (key_col) := null_replacement]
              if (debug) cat("[DEBUG] Replaced NULL values with", null_replacement, "\n")
            }
          }
        }
      }
      
      # Convert date/time columns
      if (!is.null(date_columns) || !is.null(date_time_columns)) {
        if (debug) cat("[DEBUG] Converting date/time columns\n")
        data_table <- convert_date_time_columns(data_table, date_columns, date_time_columns, debug = debug)
      }

      # Build qualified table name
      if (debug) cat("[DEBUG] Building qualified table name\n")
      qualified_table <- DBI::dbQuoteIdentifier(conn, table_name)
      if (!is.null(schema)) {
        qualified_schema <- DBI::dbQuoteIdentifier(conn, schema)
        qualified_table <- paste0(qualified_schema, ".", qualified_table)
        if (debug) cat("[DEBUG] Using schema:", schema, "\n")
      }

      # Create table
      if (debug) cat("[DEBUG] Creating table:", qualified_table, "\n")
      DBI::dbWriteTable(
        conn,
        DBI::SQL(qualified_table),
        data_table,
        row.names = FALSE,
        overwrite = TRUE
      )

      # Add primary keys if specified
      if (!is.null(primary_keys)) {
        if (debug) cat("[DEBUG] Adding primary keys:", paste(primary_keys, collapse = ","), "\n")
        pk_cmd <- paste(
          "ALTER TABLE", qualified_table,
          "ADD PRIMARY KEY (", paste(primary_keys, collapse = ","), ")"
        )
        DBI::dbExecute(conn, pk_cmd)
      }

      # Add indexes if specified
      if (!is.null(index_columns)) {
        if (debug) cat("[DEBUG] Adding indexes for columns:", paste(index_columns, collapse = ","), "\n")
        for (col in index_columns) {
          index_name <- paste0(table_name, "_", col, "_idx")
          DBI::dbExecute(
            conn,
            paste0("CREATE INDEX ", index_name, " ON ", qualified_table, " (", col, ")")
          )
        }
      }

      # Convert to hypertable if requested
      if (as_hypertable) {
        if (debug) cat("[DEBUG] Converting to hypertable with time column:", time_column, "\n")
        convert_to_hypertable(
          table_name = table_name,
          time_column = time_column,
          conn = conn,
          schema = schema,
          chunk_time_interval = chunk_time_interval,
          if_not_exists = if_not_exists,
          debug = debug
        )
      }

      if (debug) cat("[DEBUG] Table creation successful\n")
      return(TRUE)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error creating table:", e$message, "\n")
      stop(paste("Failed to create table:", e$message))
    }
  )
}

#' Append data.table to an Existing Table
#'
#' Adds new rows from a data.table to an existing table in TimescaleDB.
#' This is useful for incrementally adding new data without recreating the entire table.
#'
#' @param data_table data.table object containing the data
#' @param table_name Name of the target table
#' @param conn Database connection object (optional if global connection is initialized)
#' @param schema Schema name (optional)
#' @param on_conflict Action on conflict (default: "do nothing")
#' @return Number of rows inserted
#' @export
#' @examples
#' \dontrun{
#' # Create a new data.table with data to append
#' new_data <- data.table::data.table(
#'   time = seq(as.POSIXct("2023-05-01"), by = "1 hour", length.out = 24),
#'   value = rnorm(24),
#'   category = rep(c("A", "B"), each = 12)
#' )
#' 
#' # Using global connection with required arguments first
#' init_connection(
#'   host = "localhost", port = 5432,
#'   db = "testdb", user = "testuser", pass = "testpass"
#' )
#' 
#' # Append data with required arguments first
#' rows_added <- append_data_table_to_table(new_data, "sensor_readings")
#' 
#' close_connection()
#' 
#' # Using explicit connection as optional parameter
#' config <- configure_timescale(
#'   host = "localhost", port = 5432, 
#'   db = "testdb", user = "testuser", pass = "testpass"
#' )
#' conn <- connect_to_db(config)
#' 
#' # Append to an existing table with explicit connection
#' rows_added <- append_data_table_to_table(
#'   new_data,
#'   "sensor_readings",
#'   conn
#' )
#' 
#' disconnect_from_db(conn)
#' }
append_data_table_to_table <- function(data_table, table_name, conn = NULL,
                                      schema = NULL, on_conflict = "do nothing") {
  # Get connection if not provided
  if (is.null(conn)) {
    conn <- get_connection()
  }
  
  # Validate connection object
  if (!inherits(conn, "DBIConnection")) {
    stop("Invalid connection object: must be a DBIConnection object")
  }
  
  if (!DBI::dbIsValid(conn)) {
    stop("Invalid database connection")
  }

  tryCatch(
    {
      # Build qualified table name
      qualified_table <- DBI::dbQuoteIdentifier(conn, table_name)
      if (!is.null(schema)) {
        qualified_schema <- DBI::dbQuoteIdentifier(conn, schema)
        qualified_table <- paste0(qualified_schema, ".", qualified_table)
      }
      
      # Convert data.table to data.frame for DBI
      df <- as.data.frame(data_table)

      # Append data
      DBI::dbWriteTable(conn, DBI::SQL(qualified_table), df, append = TRUE)

      return(nrow(df))
    },
    error = function(e) {
      stop(paste("Failed to append data:", e$message))
    }
  )
}

#' Drop a Table
#'
#' Removes a table from the database. If the table is a hypertable,
#' all associated chunks will be dropped as well.
#'
#' @param table_name Name of the table to drop
#' @param conn Database connection object (optional if global connection is initialized)
#' @param schema Schema name (optional)
#' @param cascade Whether to also drop dependent objects
#' @return TRUE if successful
#' @export
#' @examples
#' \dontrun{
#' # Drop a table using global connection
#' init_connection(
#'   host = "localhost", 
#'   port = 5432,
#'   db = "testdb", 
#'   user = "testuser", 
#'   pass = "testpass"
#' )
#' drop_table("my_table")
#' close_connection()
#' 
#' # Or with explicit connection
#' conn <- connect_to_db(config)
#' drop_table("my_table", conn, cascade = TRUE)
#' disconnect_from_db(conn)
#' }
drop_table <- function(table_name, conn = NULL, schema = NULL, cascade = FALSE) {
  # Get connection if not provided
  if (is.null(conn)) {
    conn <- get_connection()
  }
  
  # Validate connection object
  if (!inherits(conn, "DBIConnection")) {
    stop("Invalid connection object: must be a DBIConnection object")
  }
  
  if (!DBI::dbIsValid(conn)) {
    stop("Invalid database connection")
  }

  tryCatch(
    {
      # Build qualified table name
      qualified_table <- DBI::dbQuoteIdentifier(conn, table_name)
      if (!is.null(schema)) {
        qualified_schema <- DBI::dbQuoteIdentifier(conn, schema)
        qualified_table <- paste0(qualified_schema, ".", qualified_table)
      }
      
      cascade_clause <- if (cascade) "CASCADE" else ""
      DBI::dbExecute(conn, paste("DROP TABLE IF EXISTS", qualified_table, cascade_clause))
      return(TRUE)
    },
    error = function(e) {
      stop(paste("Failed to drop table:", e$message))
    }
  )
}

#' Truncate a Table
#'
#' Removes all rows from a table while preserving the table structure.
#' This operation is faster than DELETE as it:
#' 1. Does not scan the table
#' 2. Does not trigger ON DELETE triggers
#' 3. Does not require VACUUM
#' 4. Does not generate WAL entries for individual row deletions
#'
#' @param table_name Name of the table to truncate
#' @param conn Database connection object (optional if global connection is initialized)
#' @param schema Schema name (optional)
#' @param cascade Whether to also truncate dependent tables
#' @return TRUE if successful
#' @export
#' @examples
#' \dontrun{
#' # Truncate a table using global connection
#' init_connection(
#'   host = "localhost", 
#'   port = 5432,
#'   db = "testdb", 
#'   user = "testuser", 
#'   pass = "testpass"
#' )
#' truncate_table("my_table")
#' close_connection()
#' 
#' # Or with explicit connection
#' conn <- connect_to_db(config)
#' truncate_table("my_table", conn, cascade = TRUE)
#' disconnect_from_db(conn)
#' }
truncate_table <- function(table_name, conn = NULL, schema = NULL, cascade = FALSE) {
  # Get connection if not provided
  if (is.null(conn)) {
    conn <- get_connection()
  }
  
  # Validate connection object
  if (!inherits(conn, "DBIConnection")) {
    stop("Invalid connection object: must be a DBIConnection object")
  }
  
  if (!DBI::dbIsValid(conn)) {
    stop("Invalid database connection")
  }

  tryCatch(
    {
      # Build qualified table name
      qualified_table <- DBI::dbQuoteIdentifier(conn, table_name)
      if (!is.null(schema)) {
        qualified_schema <- DBI::dbQuoteIdentifier(conn, schema)
        qualified_table <- paste0(qualified_schema, ".", qualified_table)
      }
      
      # Build TRUNCATE command
      truncate_cmd <- "TRUNCATE TABLE"
      if (cascade) truncate_cmd <- paste(truncate_cmd, "CASCADE")
      truncate_cmd <- paste(truncate_cmd, qualified_table)

      # Execute TRUNCATE
      DBI::dbExecute(conn, truncate_cmd)
      return(TRUE)
    },
    error = function(e) {
      stop(paste("Failed to truncate table:", e$message))
    }
  )
}

#' Rename a Table
#'
#' Changes the name of an existing table in the database.
#'
#' @param old_name Current name of the table
#' @param new_name New name for the table
#' @param conn Database connection object (optional if global connection is initialized)
#' @param schema Schema name (optional)
#' @return TRUE if successful
#' @export
#' @examples
#' \dontrun{
#' # Rename a table using global connection
#' init_connection(
#'   host = "localhost", 
#'   port = 5432,
#'   db = "testdb", 
#'   user = "testuser", 
#'   pass = "testpass"
#' )
#' rename_table("old_table_name", "new_table_name")
#' close_connection()
#' 
#' # Or with explicit connection
#' conn <- connect_to_db(config)
#' rename_table("old_table_name", "new_table_name", conn)
#' disconnect_from_db(conn)
#' }
rename_table <- function(old_name, new_name, conn = NULL, schema = NULL) {
  # Get connection if not provided
  if (is.null(conn)) {
    conn <- get_connection()
  }
  
  # Validate connection object
  if (!inherits(conn, "DBIConnection")) {
    stop("Invalid connection object: must be a DBIConnection object")
  }
  
  if (!DBI::dbIsValid(conn)) {
    stop("Invalid database connection")
  }

  tryCatch(
    {
      # Build qualified table names
      qualified_old <- DBI::dbQuoteIdentifier(conn, old_name)
      qualified_new <- DBI::dbQuoteIdentifier(conn, new_name)
      
      if (!is.null(schema)) {
        qualified_schema <- DBI::dbQuoteIdentifier(conn, schema)
        qualified_old <- paste0(qualified_schema, ".", qualified_old)
      }
      
      DBI::dbExecute(conn, paste("ALTER TABLE", qualified_old, "RENAME TO", qualified_new))
      return(TRUE)
    },
    error = function(e) {
      stop(paste("Failed to rename table:", e$message))
    }
  )
}

#' Copy a Table
#'
#' Creates a copy of a table with optional data cloning.
#'
#' @param source_table Name of the source table to copy
#' @param target_table Name of the target table to create
#' @param conn Database connection object (optional if global connection is initialized)
#' @param source_schema Schema for source table (optional)
#' @param target_schema Schema for target table (optional, defaults to source_schema if provided)
#' @param with_data Whether to copy the data (default: TRUE)
#' @param as_hypertable If TRUE and source is a hypertable, convert target to hypertable
#' @param time_column Time column name for hypertable conversion (required if as_hypertable is TRUE)
#' @param chunk_time_interval Interval for chunking (e.g., "1 day")
#' @param if_not_exists Whether to skip if hypertable already exists (default: FALSE)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return TRUE if successful
#' @export
#' @examples
#' \dontrun{
#' # Copy a table using global connection
#' init_connection(
#'   host = "localhost", 
#'   port = 5432,
#'   db = "testdb", 
#'   user = "testuser", 
#'   pass = "testpass"
#' )
#' copy_table("source_table", "target_table", with_data = TRUE)
#' close_connection()
#' 
#' # Or with explicit connection
#' conn <- connect_to_db(config)
#' copy_table("source_table", "target_table", conn, with_data = FALSE)
#' disconnect_from_db(conn)
#' }
copy_table <- function(source_table, target_table, conn = NULL,
                      source_schema = NULL, target_schema = NULL,
                      with_data = TRUE, as_hypertable = TRUE,
                      time_column = NULL, chunk_time_interval = "1 day", 
                      if_not_exists = FALSE, debug = FALSE) {
  # Get connection if not provided
  if (is.null(conn)) {
    conn <- get_connection()
  }
  
  # Validate connection object
  if (!inherits(conn, "DBIConnection")) {
    stop("Invalid connection object: must be a DBIConnection object")
  }
  
  if (!DBI::dbIsValid(conn)) {
    stop("Invalid database connection")
  }
  
  # Check if as_hypertable is TRUE but time_column is NULL
  if (as_hypertable && is.null(time_column)) {
    stop("time_column must be specified when as_hypertable is TRUE")
  }

  tryCatch(
    {
      # Build qualified table names
      qualified_source <- DBI::dbQuoteIdentifier(conn, source_table)
      if (!is.null(source_schema)) {
        qualified_source_schema <- DBI::dbQuoteIdentifier(conn, source_schema)
        qualified_source <- paste0(qualified_source_schema, ".", qualified_source)
      }
      
      qualified_target <- DBI::dbQuoteIdentifier(conn, target_table)
      if (!is.null(target_schema)) {
        qualified_target_schema <- DBI::dbQuoteIdentifier(conn, target_schema)
        qualified_target <- paste0(qualified_target_schema, ".", qualified_target)
      }
      
      # Create table structure
      DBI::dbExecute(conn, paste(
        "CREATE TABLE", qualified_target,
        "AS TABLE", qualified_source,
        "WITH NO DATA"
      ))

      # Copy data if requested
      if (with_data) {
        DBI::dbExecute(conn, paste(
          "INSERT INTO", qualified_target,
          "SELECT * FROM", qualified_source
        ))
      }

      # Convert to hypertable if requested
      if (as_hypertable && !is.null(time_column)) {
        if (debug) cat("[DEBUG] Converting to hypertable\n")
        convert_to_hypertable(
          table_name = target_table,
          time_column = time_column,
          conn = conn,
          schema = target_schema,
          chunk_time_interval = chunk_time_interval,
          if_not_exists = if_not_exists,
          debug = debug
        )
      }

      return(TRUE)
    },
    error = function(e) {
      stop(paste("Failed to copy table:", e$message))
    }
  )
}

#' Check if a Table Exists
#'
#' Determines whether a specified table exists in the database.
#'
#' @param table_name Name of the table to check
#' @param conn Database connection object (optional if global connection is initialized)
#' @param schema Schema name (default: "public")
#' @return TRUE if table exists, FALSE otherwise
#' @export
#' @examples
#' \dontrun{
#' # Check if a table exists using global connection
#' init_connection(
#'   host = "localhost", 
#'   port = 5432,
#'   db = "testdb", 
#'   user = "testuser", 
#'   pass = "testpass"
#' )
#' exists <- table_exists("my_table")
#' print(exists)
#' close_connection()
#' 
#' # Or with explicit connection
#' conn <- connect_to_db(config)
#' exists <- table_exists("my_table", conn, schema = "custom_schema")
#' print(exists)
#' disconnect_from_db(conn)
#' }
table_exists <- function(table_name, conn = NULL, schema = "public") {
  # Get connection if not provided
  if (is.null(conn)) {
    conn <- get_connection()
  }
  
  # Validate connection object
  if (!inherits(conn, "DBIConnection")) {
    stop("Invalid connection object: must be a DBIConnection object")
  }
  
  if (!DBI::dbIsValid(conn)) {
    stop("Invalid database connection")
  }

  tryCatch(
    {
      query <- paste0("
      SELECT EXISTS (
        SELECT FROM information_schema.tables
        WHERE table_schema = ", DBI::dbQuoteString(conn, schema), "
        AND table_name = ", DBI::dbQuoteString(conn, table_name), "
      )")
      
      result <- DBI::dbGetQuery(conn, query)
      return(result$exists[1])
    },
    error = function(e) {
      stop(paste("Failed to check if table exists:", e$message))
    }
  )
}

#' Get table structure
#'
#' Retrieves the structure of a table, including column names, data types,
#' nullability, and default values.
#'
#' @param table_name Name of the table
#' @param auto_reconnect Whether to attempt reconnection if the connection is lost (default: TRUE)
#' @param debug Enable verbose debug output (default: FALSE)
#' @return Data frame with column information
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
#' # Get table structure
#' structure <- get_table_structure("my_table")
#' print(structure)
#' }
get_table_structure <- function(table_name, auto_reconnect = TRUE, debug = FALSE) {
  if (debug) message("get_table_structure: Getting structure for table ", table_name)
  
  # Get connection with automatic reconnection if needed
  conn <- ensure_valid_connection(auto_reconnect, debug)

  tryCatch(
    {
      if (debug) message("get_table_structure: Executing query")
      result <- DBI::dbGetQuery(conn, "
      SELECT
        column_name,
        data_type,
        is_nullable,
        column_default
      FROM information_schema.columns
      WHERE table_schema = current_schema()
      AND table_name = $1
      ORDER BY ordinal_position
    ", params = list(table_name))
      
      if (debug) {
        message("get_table_structure: Query executed successfully")
        message("get_table_structure: Retrieved ", nrow(result), " columns")
      }
      
      return(result)
    },
    error = function(e) {
      error_msg <- paste("Failed to get table structure:", e$message)
      if (debug) message("get_table_structure: ERROR - ", error_msg)
      stop(error_msg)
    }
  )
}

#' Get table size
#'
#' Retrieves the size information for a table, including total size,
#' table size (without indexes), and index size.
#'
#' @param table_name Name of the table
#' @param auto_reconnect Whether to attempt reconnection if the connection is lost (default: TRUE)
#' @param debug Enable verbose debug output (default: FALSE)
#' @return List with size information
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
#' # Get table size
#' size_info <- get_table_size("my_table")
#' print(size_info)
#' }
get_table_size <- function(table_name, auto_reconnect = TRUE, debug = FALSE) {
  if (debug) message("get_table_size: Getting size information for table ", table_name)
  
  # Get connection with automatic reconnection if needed
  conn <- ensure_valid_connection(auto_reconnect, debug)

  tryCatch(
    {
      if (debug) message("get_table_size: Executing query")
      result <- DBI::dbGetQuery(conn, "
      SELECT
        pg_size_pretty(pg_total_relation_size($1)) as total_size,
        pg_size_pretty(pg_relation_size($1)) as table_size,
        pg_size_pretty(pg_total_relation_size($1) - pg_relation_size($1)) as index_size
    ", params = list(table_name))
      
      if (debug) {
        message("get_table_size: Query executed successfully")
        message("get_table_size: Total size = ", result$total_size)
      }
      
      return(result)
    },
    error = function(e) {
      error_msg <- paste("Failed to get table size:", e$message)
      if (debug) message("get_table_size: ERROR - ", error_msg)
      stop(error_msg)
    }
  )
}


#' Convert a regular table to a hypertable
#'
#' Converts a regular PostgreSQL table to a TimescaleDB hypertable.
#' A hypertable is a table that is automatically partitioned by time.
#'
#' @param conn Database connection object (optional if global connection is initialized)
#' @param table_name Name of the table to convert
#' @param time_column Name of the time column
#' @param schema Schema name (optional)
#' @param chunk_time_interval Interval for chunking (default: "1 day")
#' @param if_not_exists Whether to skip if hypertable already exists (default: FALSE)
#' @param migrate_data Whether to migrate data to the new hypertable (default: TRUE)
#' @param auto_reconnect Whether to attempt reconnection if the connection is lost (default: TRUE)
#' @param debug Enable verbose debug output (default: FALSE)
#' @return TRUE if conversion was successful
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
#' # Convert a table to hypertable
#' convert_to_hypertable(conn, "my_table", "time_column",
#'   schema = "public", chunk_time_interval = "1 day"
#' )
#' }
convert_to_hypertable <- function(table_name, time_column, conn = NULL,
                                 schema = NULL, chunk_time_interval = "1 day", 
                                 if_not_exists = FALSE, migrate_data = TRUE,
                                 auto_reconnect = TRUE, debug = FALSE) {
  if (debug) {
    message("convert_to_hypertable: Converting table ", table_name, " to hypertable")
    message("convert_to_hypertable: time_column = ", time_column, 
           ", schema = ", if(is.null(schema)) "NULL" else schema, 
           ", chunk_time_interval = ", chunk_time_interval,
           ", if_not_exists = ", if_not_exists)
  }
  
  # Get connection with automatic reconnection if needed
  if (is.null(conn)) {
    if (debug) cat("[DEBUG] Using global connection\n")
    conn <- get_connection()
  }
  
  if (!inherits(conn, "DBIConnection") || !DBI::dbIsValid(conn)) {
    if (auto_reconnect) {
      if (debug) message("convert_to_hypertable: Attempting to reconnect...")
      conn <- fix_connection(NULL)
    } else {
      stop("Invalid database connection")
    }
  }

  tryCatch(
    {
      # Build qualified table name
      qualified_table <- DBI::dbQuoteIdentifier(conn, table_name)
      if (!is.null(schema)) {
        qualified_schema <- DBI::dbQuoteIdentifier(conn, schema)
        qualified_table <- paste0(qualified_schema, ".", qualified_table)
      }
      
      if (debug) message("convert_to_hypertable: Using qualified table name: ", qualified_table)

      # Build CREATE HYPERTABLE command
      cmd <- paste("SELECT create_hypertable(", 
                  paste0("'", qualified_table, "'"), ",", 
                  paste0("'", time_column, "'"), ",",
                  paste0("'", chunk_time_interval, "'"))
      if (if_not_exists) cmd <- paste(cmd, ", if_not_exists => TRUE")
      cmd <- paste0(cmd, ")")
      
      if (debug) message("convert_to_hypertable: Executing command: ", cmd)

      # Execute command
      DBI::dbExecute(conn, cmd)
      
      if (debug) message("convert_to_hypertable: Table successfully converted to hypertable")
      return(TRUE)
    },
    error = function(e) {
      error_msg <- paste("Failed to convert to hypertable:", e$message)
      if (debug) message("convert_to_hypertable: ERROR - ", error_msg)
      stop(error_msg)
    }
  )
}

#' Vacuum a Table
#'
#' Performs a VACUUM operation on a table to reclaim storage occupied by dead tuples.
#' VACUUM is a PostgreSQL maintenance operation that:
#' 1. Removes dead tuples (rows that are no longer visible to any transaction)
#' 2. Updates statistics used by the query planner
#' 3. Updates the visibility map to speed up index-only scans
#' 4. Prevents transaction ID wraparound
#'
#' @param table_name Name of the table to vacuum
#' @param analyze Logical, whether to update statistics (default: TRUE)
#' @param verbose Logical, whether to print detailed information (default: FALSE)
#' @param full Logical, whether to perform a full vacuum (default: FALSE)
#' @param auto_reconnect Whether to attempt reconnection if the connection is lost (default: TRUE)
#' @param debug Enable verbose debug output (default: FALSE)
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
#' # Vacuum a table
#' vacuum_table("my_table", analyze = TRUE)
#' }
vacuum_table <- function(table_name, analyze = TRUE, verbose = FALSE, full = FALSE,
                        auto_reconnect = TRUE, debug = FALSE) {
  if (debug) {
    message("vacuum_table: Starting vacuum operation on ", table_name)
    message("vacuum_table: analyze = ", analyze, ", verbose = ", verbose, ", full = ", full)
  }
  
  # Get connection with automatic reconnection if needed
  conn <- ensure_valid_connection(auto_reconnect, debug)

  tryCatch(
    {
      # Build VACUUM command
      vacuum_cmd <- "VACUUM"
      if (full) vacuum_cmd <- paste(vacuum_cmd, "FULL")
      if (analyze) vacuum_cmd <- paste(vacuum_cmd, "ANALYZE")
      if (verbose) vacuum_cmd <- paste(vacuum_cmd, "VERBOSE")
      vacuum_cmd <- paste(vacuum_cmd, table_name)
      
      if (debug) message("vacuum_table: Executing command: ", vacuum_cmd)

      # Execute VACUUM
      DBI::dbExecute(conn, vacuum_cmd)
      
      if (debug) message("vacuum_table: Vacuum operation completed successfully")
      return(TRUE)
    },
    error = function(e) {
      error_msg <- paste("Failed to vacuum table:", e$message)
      if (debug) message("vacuum_table: ERROR - ", error_msg)
      stop(error_msg)
    }
  )
}

#' Analyze a Table
#' 
#' Analyzes a table to update statistics used by the query planner.
#' ANALYZE is a PostgreSQL operation that collects statistics about the contents
#' of tables in the database, which helps the query planner generate better execution plans.
#' 
#' @param table_name Name of the table to analyze
#' @param verbose Logical, whether to print detailed information (default: FALSE)
#' @param auto_reconnect Whether to attempt reconnection if the connection is lost (default: TRUE)
#' @param debug Enable verbose debug output (default: FALSE)
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
#' # Analyze a table
#' analyze_table("my_table")
#' 
#' # Analyze with verbose output
#' analyze_table("my_table", verbose = TRUE)
#' }
analyze_table <- function(table_name, verbose = FALSE, auto_reconnect = TRUE, debug = FALSE) {
  if (debug) {
    message("analyze_table: Starting analyze operation on ", table_name)
    message("analyze_table: verbose = ", verbose)
  }
  
  # Get connection with automatic reconnection if needed
  conn <- ensure_valid_connection(auto_reconnect, debug)

  tryCatch(
    {
      # Build ANALYZE command
      analyze_cmd <- "ANALYZE"
      if (verbose) analyze_cmd <- paste(analyze_cmd, "VERBOSE")
      analyze_cmd <- paste(analyze_cmd, table_name)
      
      if (debug) message("analyze_table: Executing command: ", analyze_cmd)

      # Execute ANALYZE
      DBI::dbExecute(conn, analyze_cmd)
      
      if (debug) message("analyze_table: Analyze operation completed successfully")
      return(TRUE)
    },
    error = function(e) {
      error_msg <- paste("Failed to analyze table:", e$message)
      if (debug) message("analyze_table: ERROR - ", error_msg)
      stop(error_msg)
    }
  )
}

#' List all hypertables in the database
#'
#' Retrieves a list of all TimescaleDB hypertables in the current database.
#'
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param auto_reconnect Whether to attempt reconnection if the connection is lost (default: TRUE)
#' @return Data frame with hypertable information
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
#' # List all hypertables
#' hypertables <- list_hypertables(conn = conn)
#' print(hypertables)
#' }
list_hypertables <- function(conn = NULL, auto_reconnect = TRUE) {
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
    conn <- ensure_valid_connection(auto_reconnect)
  }

  tryCatch(
    {
      query <- "
      SELECT h.schema_name, 
             h.table_name, 
             h.time_column_name, 
             h.chunk_time_interval,
             COALESCE(pg_size_pretty(pg_table_size(format('%I.%I', h.schema_name, h.table_name)::regclass)), '0 B') as table_size
      FROM _timescaledb_catalog.hypertable h
      ORDER BY h.schema_name, h.table_name
      "
      
      DBI::dbGetQuery(conn, query)
    },
    error = function(e) {
      stop(paste("Failed to list hypertables:", e$message))
    }
  )
}
