# TimescaleR Basic Usage Example
# This script demonstrates how to use the TimescaleR package

# Load the package
library(TimescaleR)

# Method 1: Global connection with debug output
# ----------------------------------------------
cat("\n=== Method 1: Global connection with full library loading ===\n")

# Initialize connection with debug output
init_connection(
  host = "localhost",
  port = 5432, 
  db = "testdb",
  user = "testuser",
  pass = "testpass",
  schema = "public",
  debug = TRUE
)

# Execute a query using the global connection
cat("Executing query with global connection...\n")
tryCatch({
  result <- get_query("SELECT 1 as test", debug = TRUE)
  print(result)
}, error = function(e) {
  cat("Query error:", e$message, "\n")
})

# Close the connection
cat("Closing connection...\n")
close_connection(debug = TRUE)

# Method 2: Namespace qualification
# --------------------------------
cat("\n=== Method 2: Using namespace qualification ===\n")

# Initialize connection with namespace qualification
TimescaleR::init_connection(
  host = "localhost",
  port = 5432, 
  db = "testdb",
  user = "testuser",
  pass = "testpass",
  schema = "public",
  debug = TRUE
)

# Execute a query using namespace qualification
cat("Executing query with namespace qualification...\n")
tryCatch({
  result <- TimescaleR::get_query("SELECT 1 as test", debug = TRUE)
  print(result)
}, error = function(e) {
  cat("Query error:", e$message, "\n")
})

# Check connection status
cat("Checking connection status...\n")
has_conn <- TimescaleR::has_connection()
cat("Has connection:", has_conn, "\n")

# Debug environment
cat("\n=== Debugging environment ===\n")
TimescaleR::debug_environment(verbose = TRUE)

# Close connection
cat("Closing connection...\n")
TimescaleR::close_connection(debug = TRUE)

# Method 3: Fixing connection issues
# ---------------------------------
cat("\n=== Method 3: Fixing connection issues ===\n")

# Fix connection with explicit config
cat("Attempting to fix connection...\n")
config <- list(
  host = "localhost",
  port = 5432,
  db = "testdb",
  user = "testuser",
  pass = "testpass",
  schema = "public"
)
TimescaleR::fix_connection(config)

# Try a query after fixing
cat("Executing query after fixing connection...\n")
tryCatch({
  result <- TimescaleR::get_query("SELECT 1 as test", debug = TRUE)
  print(result)
}, error = function(e) {
  cat("Query error:", e$message, "\n")
})

# Close connection
cat("Closing connection...\n")
TimescaleR::close_connection(debug = TRUE) 
