setwd("/home/jochen/MoneroPoolScraper")
library(jsonlite)
library(DBI)
library(dplyr)
library(R.utils)
source("./DButils.R")

con <- dbConnect(RMariaDB::MariaDB(), user=DB_USERNAME, password=DB_PW, dbname="monero")

# General Setup
POOL_NAME <- "Zergpool"
POOL_URL <- "https://zergpool.com"

# Define API-Endpoints to query
BLOCKS_ENDPOINT <- "https://api.zergpool.com:8443/api/blocks?coin=XMR"


# Rate Limit (Max API Hits per Minute)
RATE_LIMIT <- 30

# Preparation: Check if Pool is known to Database (required as Miner references the Pool Name as Foreign Key)
check_pool(POOL_NAME, POOL_URL)

# Preparation: Check for newest known block
lastKnownBlock <- get_latest_poolblock(POOL_NAME)

# Retrieve mined Blocks

minedBlocks <- fromJSON(BLOCKS_ENDPOINT)
minedBlocks <- minedBlocks[minedBlocks$height>lastKnownBlock,]

if(nrow(minedBlocks)>0){
  minedBlocks <- minedBlocks[,c("height","time", "amount", "difficulty")]
  names(minedBlocks) <- c("Height", "Date", "Reward", "Difficulty")
  minedBlocks$Date <- as.POSIXct(as.numeric(minedBlocks$Date), origin="1970-01-01", tz="GMT")
  minedBlocks$Pool <- POOL_NAME
  minedBlocks$Timestamp <- Sys.time()
  
  mysql_fast_db_write_table(con, "block",minedBlocks, append = TRUE)
}


dbDisconnect(con)
