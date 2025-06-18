#' Insert data into a table in batches
#'
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param table_name Name of the table
#' @param data Data to insert
#' @param batch_size Size of each batch (default: 1000)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return Number of rows inserted
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
#' data <- data.frame(time = Sys.time() + 1:100, value = rnorm(100))
#' insert_data_to_table(table_name = "sensor_data", data = data)
#' 
#' # Or with explicit connection
#' conn <- get_connection()
#' insert_data_to_table(conn, "sensor_data", data)
#' }
insert_data_to_table <- function(conn = NULL, table_name, data, batch_size = 1000, debug = FALSE) {
  if (debug) cat("[DEBUG] Inserting data into table:", table_name, "\n")
  
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
      # Split data into batches
      n_batches <- ceiling(nrow(data) / batch_size)
      total_inserted <- 0
      
      if (debug) cat("[DEBUG] Inserting", nrow(data), "rows in", n_batches, "batches of size", batch_size, "\n")

      for (i in 1:n_batches) {
        start_idx <- (i - 1) * batch_size + 1
        end_idx <- min(i * batch_size, nrow(data))
        batch <- data[start_idx:end_idx, ]
        
        if (debug) cat("[DEBUG] Inserting batch", i, "of", n_batches, "(rows", start_idx, "to", end_idx, ")\n")

        # Insert batch
        DBI::dbWriteTable(conn, table_name, batch, append = TRUE)
        total_inserted <- total_inserted + nrow(batch)
      }
      
      if (debug) cat("[DEBUG] Successfully inserted", total_inserted, "rows\n")
      return(total_inserted)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error inserting data:", e$message, "\n")
      stop(paste("Failed to insert data:", e$message))
    }
  )
}


#' Insert data with conflict handling
#'
#' Inserts data into a database table with support for handling conflicts.
#' This function allows specifying the conflict resolution strategy when
#' unique or primary key constraints would be violated.
#'
#' @param conn Database connection object (optional, uses global connection if NULL)
#' @param table_name Name of the table
#' @param data A data frame to insert. Each row represents a record.
#' @param conflict_target Column(s) to check for conflicts, as a string or vector
#' @param conflict_action Action to take on conflict (default: "do nothing").
#'   Can also be "DO UPDATE SET col1 = EXCLUDED.col1, ..." to update specific columns.
#' @param debug Enable debug messages (optional, default FALSE)
#' @return Number of rows processed for insertion
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
#' 
#' # Single row insertion
#' data <- data.frame(id = 1, value = 42)
#' insert_with_conflict_handling(
#'   table_name = "my_table", 
#'   data = data, 
#'   conflict_target = "id"
#' )
#' 
#' # Multi-row insertion
#' data <- data.frame(id = 1:5, value = rnorm(5))
#' insert_with_conflict_handling(
#'   table_name = "my_table", 
#'   data = data, 
#'   conflict_target = "id"
#' )
#' }
insert_with_conflict_handling <- function(conn = NULL, table_name, data, conflict_target,
                                          conflict_action = "do nothing", debug = FALSE) {
  if (debug) cat("[DEBUG] Inserting data with conflict handling into table:", table_name, "\n")
  
  # Use global connection if not provided
  if (is.null(conn)) {
    if (debug) cat("[DEBUG] Using global connection\n")
    conn <- get_connection()
  }
  
  if (!DBI::dbIsValid(conn)) {
    stop("Invalid database connection")
  }
  
  # Format conflict target if multiple columns provided
  if (length(conflict_target) > 1) {
    conflict_target <- paste(conflict_target, collapse = ", ")
  }
  
  tryCatch(
    {
      # Build INSERT statement with conflict handling
      if (debug) {
        cat("[DEBUG] Building INSERT statement with conflict handling\n")
        cat("[DEBUG] Conflict target:", conflict_target, "\n")
        cat("[DEBUG] Conflict action:", conflict_action, "\n")
      }
      
      # Ensure data is a data.frame
      if (!is.data.frame(data)) {
        data <- as.data.frame(data)
      }
      
      # Get column names and create placeholder string
      col_names <- names(data)
      
      # Create the SQL statement with properly formatted PostgreSQL parameter placeholders
      # PostgreSQL uses $1, $2, etc. for parameter binding
      if (nrow(data) == 1) {
        # For single row
        placeholders <- paste0("(", paste0("$", 1:length(col_names), collapse = ", "), ")")
      } else {
        # For multiple rows, we'll use UNNEST for bulk insertion
        # This creates a more efficient multi-row insert in PostgreSQL
        unnest_placeholders <- paste0(
          "UNNEST(ARRAY[", 
          paste0(lapply(1:nrow(data), function(row_idx) {
            start_idx <- (row_idx - 1) * length(col_names) + 1
            end_idx <- row_idx * length(col_names)
            paste0("$", start_idx:end_idx, collapse = ", ")
          }), collapse = "]), ARRAY["),
          "])"
        )
        placeholders <- paste0("SELECT * FROM ", unnest_placeholders)
      }
      
      # Build the final SQL query
      if (nrow(data) == 1) {
        insert_sql <- paste(
          "INSERT INTO", table_name,
          "(", paste(col_names, collapse = ", "), ")",
          "VALUES", placeholders,
          paste0("ON CONFLICT (", conflict_target, ") ", toupper(conflict_action))
        )
        # Flatten the single row into a list of values
        params <- as.list(data[1,])
      } else {
        # Multi-row insert with UNNEST approach
        insert_sql <- paste(
          "INSERT INTO", table_name,
          "(", paste(col_names, collapse = ", "), ")",
          "SELECT * FROM (VALUES",
          paste0(
            lapply(1:nrow(data), function(i) {
              param_indices <- ((i-1) * ncol(data) + 1):(i * ncol(data))
              paste0("(", paste0("$", param_indices, collapse = ", "), ")")
            }),
            collapse = ",\n"
          ),
          ") AS t(", paste(col_names, collapse = ", "), ")",
          paste0("ON CONFLICT (", conflict_target, ") ", toupper(conflict_action))
        )
        
        # Flatten all rows into a single list of values
        params <- as.list(unlist(lapply(1:nrow(data), function(i) as.list(data[i,]))))
      }
      
      if (debug) {
        cat("[DEBUG] SQL Query:", insert_sql, "\n")
        cat("[DEBUG] Executing INSERT with conflict handling for", nrow(data), "rows\n")
        cat("[DEBUG] Parameters count:", length(params), "\n")
      }
      
      # Execute the query
      rows_affected <- DBI::dbExecute(conn, insert_sql, params = params)
      
      if (debug) cat("[DEBUG] Successfully inserted data. Rows affected:", rows_affected, "\n")
      return(rows_affected)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error inserting data with conflict handling:", e$message, "\n")
      stop(paste("Failed to insert data with conflict handling:", e$message))
    }
  )
}


#' Adapt data types for database insertion
#'
#' @param data Data to adapt
#' @param debug Enable debug messages (optional, default FALSE)
#' @return Adapted data
#' @export
#'
#' @examples
#' data <- data.frame(
#'   timestamp = Sys.time() + 1:5,
#'   date = Sys.Date() + 1:5,
#'   value = rnorm(5)
#' )
#' adapted_data <- adapt_data_types(data)
adapt_data_types <- function(data, debug = FALSE) {
  if (debug) cat("[DEBUG] Adapting data types for database insertion\n")
  
  tryCatch(
    {
      # Convert POSIXct to character
      posix_cols <- sapply(data, function(x) inherits(x, "POSIXct"))
      if (any(posix_cols)) {
        if (debug) cat("[DEBUG] Converting POSIXct columns to character:", paste(names(data)[posix_cols], collapse = ", "), "\n")
        data[, posix_cols] <- lapply(data[, posix_cols, drop = FALSE], as.character)
      }

      # Convert Date to character
      date_cols <- sapply(data, function(x) inherits(x, "Date"))
      if (any(date_cols)) {
        if (debug) cat("[DEBUG] Converting Date columns to character:", paste(names(data)[date_cols], collapse = ", "), "\n")
        data[, date_cols] <- lapply(data[, date_cols, drop = FALSE], as.character)
      }
      
      if (debug) cat("[DEBUG] Data types adapted successfully\n")
      return(data)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error adapting data types:", e$message, "\n")
      stop(paste("Failed to adapt data types:", e$message))
    }
  )
}

#' Clean data for primary keys
#'
#' @param data Data to clean
#' @param primary_keys Vector of primary key column names
#' @param debug Enable debug messages (optional, default FALSE)
#' @return Cleaned data
#' @export
#'
#' @examples
#' data <- data.frame(
#'   id = c(1, 2, NA, 4, 5),
#'   value = rnorm(5)
#' )
#' cleaned_data <- clean_data_for_primary_keys(data, "id")
clean_data_for_primary_keys <- function(data, primary_keys, debug = FALSE) {
  if (debug) cat("[DEBUG] Cleaning data for primary keys:", paste(primary_keys, collapse = ", "), "\n")
  
  tryCatch(
    {
      # Remove rows with NULL primary keys
      null_pk <- sapply(data[primary_keys], function(x) any(is.na(x)))
      if (any(null_pk)) {
        original_rows <- nrow(data)
        data <- data[!apply(data[primary_keys], 1, function(x) any(is.na(x))), ]
        if (debug) cat("[DEBUG] Removed", original_rows - nrow(data), "rows with NULL primary keys\n")
      } else {
        if (debug) cat("[DEBUG] No rows with NULL primary keys found\n")
      }
      
      if (debug) cat("[DEBUG] Data cleaned successfully, returning", nrow(data), "rows\n")
      return(data)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error cleaning data for primary keys:", e$message, "\n")
      stop(paste("Failed to clean data for primary keys:", e$message))
    }
  )
}

#' Determine column types for table creation
#'
#' @param data Data to analyze
#' @param debug Enable debug messages (optional, default FALSE)
#' @return Vector of SQL types
#' @export
#'
#' @examples
#' data <- data.frame(
#'   timestamp = Sys.time(),
#'   date = Sys.Date(),
#'   number = 123.45,
#'   flag = TRUE,
#'   text = "example"
#' )
#' col_types <- determine_column_types(data)
determine_column_types <- function(data, debug = FALSE) {
  if (debug) cat("[DEBUG] Determining column types for table creation\n")
  
  tryCatch(
    {
      types <- sapply(data, function(x) {
        if (inherits(x, "POSIXct")) {
          "TIMESTAMP"
        } else if (inherits(x, "Date")) {
          "DATE"
        } else if (is.numeric(x)) {
          "DOUBLE PRECISION"
        } else if (is.logical(x)) {
          "BOOLEAN"
        } else {
          "TEXT"
        }
      })
      
      if (debug) {
        for (col_name in names(types)) {
          cat("[DEBUG] Column", col_name, "->", types[col_name], "\n")
        }
      }
      
      return(types)
    },
    error = function(e) {
      if (debug) cat("[DEBUG] Error determining column types:", e$message, "\n")
      stop(paste("Failed to determine column types:", e$message))
    }
  )
}

#' Convert date/time columns in a data.table
#'
#' Converts specified columns in a data.table to DATE or TIMESTAMP type.
#'
#' @param data A data.table object
#' @param date_columns Vector of column names to convert to DATE type (optional)
#' @param date_time_columns Vector of column names to convert to TIMESTAMP type (optional)
#' @param debug Enable debug messages (optional, default FALSE)
#' @return A data.table with converted date/time columns
#' @export
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
#' data <- data.table(
#'   date = as.Date("2024-01-01"),
#'   datetime = as.POSIXct("2024-01-01 12:00:00")
#' )
#' converted <- convert_date_time_columns(data,
#'   date_columns = "date",
#'   date_time_columns = "datetime"
#' )
#' }
convert_date_time_columns <- function(data, date_columns = NULL, date_time_columns = NULL, debug = FALSE) {
  # Validate input
  if (is.null(data)) {
    stop("Data cannot be NULL")
  }
  
  if (!is.data.frame(data) && !data.table::is.data.table(data)) {
    stop("Data must be a data.frame or data.table")
  }
  
  tryCatch({
    if (!data.table::is.data.table(data)) {
      if (debug) cat("[DEBUG] Input is not a data.table, converting\n")
      data <- data.table::as.data.table(data)
    }
  }, error = function(e) {
    stop(paste("Failed to convert data to data.table:", e$message))
  })

  if (!is.null(date_columns)) {
    if (debug) cat("[DEBUG] Converting date columns:", paste(date_columns, collapse=", "), "\n")
    for (col in date_columns) {
      if (col %in% names(data)) {
        data[[col]] <- as.Date(data[[col]])
      }
    }
  }

  if (!is.null(date_time_columns)) {
    if (debug) cat("[DEBUG] Converting datetime columns:", paste(date_time_columns, collapse=", "), "\n")
    for (col in date_time_columns) {
      if (col %in% names(data)) {
        data[[col]] <- as.POSIXct(data[[col]])
      }
    }
  }
  
  return(data)
}

#' Initialize connection
#'
#' @param conn Database connection object
#' @param table_name Name of the table
#' @param debug Enable debug messages (optional, default FALSE)
#' @return TRUE if successful
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
#' # Your table_utils examples here
#'
#' # Or with explicit connection
#' conn <- get_connection()
#' # Your table_utils examples here
#' }

#' Other table_utils function
#'
#' @param conn Database connection object
#' @param table_name Name of the table
#' @param debug Enable debug messages (optional, default FALSE)
#' @return TRUE if successful
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
#' # Your other table_utils examples here
#'
#' # Or with explicit connection
#' conn <- get_connection()
#' # Your other table_utils examples here
#' }
