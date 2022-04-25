# Login Credentials

DB_USERNAME <- "root"
DB_PW <- "ddjFTW!"

# Workaround because of slow DB Writes
# https://github.com/r-dbi/RMariaDB/issues/162#issuecomment-689045252
mysql_fast_db_write_table <- function(conn, name, value, field.types = NULL,  append = FALSE, ...) {
  # create the table if needed
  if (!DBI::dbExistsTable(conn, name) || !append) {
    # fix for the field types
    if (is.null(field.types)) {
      field.types <- vapply(value, DBI::dbDataType, dbObj = conn, FUN.VALUE = character(1))
    }
    DBI::dbWriteTable(conn, name, value[NULL,], field.types = field.types, ...)
  }
  
  if (nrow(value) == 0) return(0)
  
  ### write a temp csv file
  fn <- normalizePath(tempfile("rsdbi"), winslash = "/",  mustWork = FALSE)
  RMySQL:::safe.write(value, file = fn)
  on.exit(unlink(fn), add = TRUE)
  
  mysql_load_data_local_infile(conn, name, path = fn, cols = names(value), header = FALSE, sep = '\t')
  #DBI::dbWriteTable(conn, name, fn, append = TRUE, overwrite = FALSE, header = FALSE, sep = '\t')
}

mysql_load_data_local_infile <- function(conn, name, path, cols, header = TRUE, sep = ",", eol = "\n", skip = 0, quote = "\"") {
  .quote_id <- function(x) DBI::dbQuoteIdentifier(conn ,x)
  .quote_s <- function(x) DBI::dbQuoteString(conn, x)
  
  cols <- sapply(cols,  .quote_id)
  cols <- paste0(cols, collapse = ", ")
  
  sql <- paste0(
    "LOAD DATA LOCAL INFILE ", .quote_s( path), "\n", 
    "INTO TABLE ", .quote_id(name), "\n", 
    "FIELDS TERMINATED BY ", .quote_s(sep), "\n", 
    "OPTIONALLY ENCLOSED BY ", .quote_s(quote), "\n", 
    "LINES TERMINATED BY ", .quote_s(eol), "\n", 
    "IGNORE ", skip + as.integer(header), " LINES\n", 
    "(", cols, ");"
  )
  
  DBI::dbExecute(conn, sql)
}



# Check if Pool is known and create if neccessary

check_pool <- function(poolName, poolURL){
  res <- dbSendQuery(con, paste0("SELECT * FROM pool WHERE Name = '",poolName,"'"))
  rows <- dbFetch(res)
  dbClearResult(res)
  
  # If not known, create
  
  if(nrow(rows)==0){
    message("Pool added to database")
    dbWriteTable(con, "pool", data.frame(Name=poolName, URL=poolURL, Timestamp=Sys.time()), append=TRUE)
  }
  rm(res, rows)
}

# Returns the height of the newest block that is known for a specific mining pool. If none are known,
# zero is returned.
get_latest_poolblock <- function(poolName){
  res <- dbSendQuery(con, paste0("SELECT MAX(Height) FROM block WHERE Pool = '",poolName,"'"))
  rows <- dbFetch(res)
  dbClearResult(res)
  lastKnownBlock <- rows[1,1]
  rm(res, rows)
  
  return(max(lastKnownBlock, 0, na.rm = TRUE))
  
}


# Returns the time of the most recent payout that is known for a specific mining pool. If none is known,
# the epoch is returned is returned.
get_latest_poolpayout <- function(poolName){
  res <- dbSendQuery(con, paste0("SELECT MAX(Date) FROM payouttransaction WHERE Pool = '",poolName,"'"))
  rows <- dbFetch(res)
  dbClearResult(res)
  lastKnownBlock <- rows[1,1]
  rm(res, rows)
  
  return(max(lastKnownBlock, as.Date("1970-01-01"), na.rm = TRUE))
  
}


# Returns known payout transactions issued by a pool
get_poolpayouttx <- function(poolName){
  res <- dbSendQuery(con, paste0("SELECT TxHash FROM payouttransaction WHERE Pool = '",poolName,"'"))
  rows <- dbFetch(res)
  dbClearResult(res)
  rm(res)
  
  return(rows)
  
}

get_poolpayouts <- function(poolName){
  res <- dbSendQuery(con, paste0("SELECT DISTINCT Miner, TxHash FROM payout WHERE Miner IN (SELECT DISTINCT Address from miner WHERE 
                                 Pool = '",poolName,"')"))
  rows <- dbFetch(res)
  dbClearResult(res)
  rm(res)
  
  return(rows)
  
}

# Returns List of all known miner addresses
get_totalminers <- function(){
  res <- dbSendQuery(con, "SELECT DISTINCT Address FROM miner")
  rows <- dbFetch(res)
  dbClearResult(res)
  rm(res)

  return(rows)
  
}

# Returns List of all known miner addresses of a pool
get_poolminers <- function(poolName){
  res <- dbSendQuery(con, paste0("SELECT DISTINCT Address FROM miner WHERE Pool = '",poolName,"'"))
  rows <- dbFetch(res)
  dbClearResult(res)
  rm(res)
  
  return(rows)
  
}

# Returns List of all known worker UIDs of a pool
get_poolworkers <- function(poolName){
  res <- dbSendQuery(con, paste0("SELECT DISTINCT UID FROM worker WHERE Pool = '",poolName,"'"))
  rows <- dbFetch(res)
  dbClearResult(res)
  rm(res)
  
  return(rows)
  
}

get_poolhashes <- function(poolName){
  res <- dbSendQuery(con, paste0("SELECT DISTINCT Miner, RefTime FROM hashrate WHERE Miner IN (SELECT DISTINCT Address from miner WHERE 
                                 Pool = '",poolName,"')"))
  rows <- dbFetch(res)
  dbClearResult(res)
  rm(res)
  
  return(rows)
  
}
