setwd("/home/jochen/MoneroPoolScraper")
library(jsonlite)
library(DBI)
library(dplyr)
library(R.utils)
source("./DButils.R")

con <- dbConnect(RMariaDB::MariaDB(), user=DB_USERNAME, password=DB_PW, dbname="monero")

# General Setup
POOL_NAME <- "MineXMR"
POOL_URL <- "https://minexmr.com"

# Define API-Endpoints to query
BLOCKS_ENDPOINT <- "https://minexmr.com/api/main/pool/blocks/day?"

# Rate Limit (Max API Hits per Minute)
RATE_LIMIT <- 6.5 # 100 per 15 Minutes

# Preparation: Check if Pool is known to Database (required as Miner references the Pool Name as Foreign Key)
check_pool(POOL_NAME, POOL_URL)

# Preparation: Check for newest known block
lastKnownBlock <- get_latest_poolblock(POOL_NAME)

minedBlocks <- fromJSON(paste0(BLOCKS_ENDPOINT,27505800))

minedBlocks <- strsplit(minedBlocks,":")
# Drop pending and orphaned blocks
minedBlocks <- minedBlocks[-which(lengths(minedBlocks)==6)]

minedBlocks <- lapply(minedBlocks, function(x) data.frame(t(x), stringsAsFactors = FALSE))
minedBlocks <- bind_rows(minedBlocks)
minedBlocks <- minedBlocks[,-c(5,6)]
names(minedBlocks) <- c("Height", "Hash", "Date", "Difficulty","Reward")
minedBlocks$Reward <- as.numeric(minedBlocks$Reward) / 10^12
minedBlocks$Date <- as.POSIXct(as.numeric(as.character(minedBlocks$Date)), origin="1970-01-01", tz="GMT")
minedBlocks$Pool <- POOL_NAME
minedBlocks$Timestamp <- Sys.time()
minedBlocks$Height <- as.numeric(minedBlocks$Height)
# Drop already known blocks
minedBlocks <- minedBlocks[minedBlocks$Height>lastKnownBlock,]

if(nrow(minedBlocks)>0){
  mysql_fast_db_write_table(con, "block",minedBlocks, append = TRUE)
}
dbDisconnect(con)
