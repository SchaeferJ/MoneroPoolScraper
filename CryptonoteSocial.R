setwd("/home/jochen/Poolscraper")
library(jsonlite)
library(DBI)
library(dplyr)
library(R.utils)
library(httr)
source("./DButils.R")
source("./ParseUtils.R")


con <- dbConnect(RMariaDB::MariaDB(), user=DB_USERNAME, password=DB_PW, dbname="monero")

# General Setup
POOL_NAME <- "Cryptonote.social"
POOL_URL <- "https://cryptonote.social"

# Define API-Endpoints to query
BLOCKS_ENDPOINT <- "https://cryptonote.social/json/MinedBlocks"

# Preparation: Check if Pool is known to Database (required as Miner references the Pool Name as Foreign Key)
check_pool(POOL_NAME, POOL_URL)

# Preparation: Check for newest known block
lastKnownBlock <- get_latest_poolblock(POOL_NAME)



req <- POST(BLOCKS_ENDPOINT, 
            body = '{ "Coin": "xmr"}')
stop_for_status(req)
json <- content(req, "text", encoding = "UTF-8")
minedBlocks <- fromJSON(json)
minedBlocks <- minedBlocks[["MinedBlocks"]]

minedBlocks <- minedBlocks[minedBlocks$Status == 0,]
minedBlocks$Status <- NULL
minedBlocks$Parent <- NULL

minedBlocks$Time <- as.POSIXct(as.numeric(minedBlocks$Time), origin="1970-01-01", tz="GMT")
minedBlocks$Pool <- POOL_NAME
minedBlocks$Timestamp <- Sys.time()

minedBlocks <- minedBlocks[minedBlocks$Height > lastKnownBlock,]

names(minedBlocks)[2] <- "Date"
mysql_fast_db_write_table(con, "block",minedBlocks, append = TRUE)
dbDisconnect(con)
