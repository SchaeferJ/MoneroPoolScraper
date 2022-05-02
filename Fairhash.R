setwd("/home/jochen/MoneroPoolScraper")
library(jsonlite)
library(DBI)
library(dplyr)
library(R.utils)
source("./DButils.R")
source("./ParseUtils.R")


con <- dbConnect(RMariaDB::MariaDB(), user=DB_USERNAME, password=DB_PW, dbname="monero")

# General Setup
POOL_NAME <- "Fairhash"
POOL_URL <- "https://monero.fairhash.org"

# Define API-Endpoints to query
STATS_ENDPOINT <- "https://monero.fairhash.org/api/stats"
BLOCKS_ENDPOINT <- "https://monero.fairhash.org/api/get_blocks?height="
PAYMENT_ENDPOINT <- "https://monero.fairhash.org/api/get_payments?time="

# Rate Limit (Max API Hits per Minute)
RATE_LIMIT <- 30

# Preparation: Check if Pool is known to Database (required as Miner references the Pool Name as Foreign Key)
check_pool(POOL_NAME, POOL_URL)

# Preparation: Check for newest known block
lastKnownBlock <- get_latest_poolblock(POOL_NAME)

# Step 1: Retrieve Pool Stats (Contains first batch of blocks and payments)

blockList <- list()
paymentList <- list()

poolStats <- fromJSON(STATS_ENDPOINT)

blockList[[1]] <- process_blockstring(poolStats[["pool"]][["blocks"]])
paymentList[[1]] <- process_paymentstring(poolStats[["pool"]][["payments"]])

# Step 2: Retrieve remaining Blocks

i <- 2
while (TRUE) {
  message(paste("Iteration",i-1))
  minedBlocks <- fromJSON(paste0(BLOCKS_ENDPOINT,min(blockList[[i-1]]$Height)))
  if(length(minedBlocks)==0 | "error" %in% names(minedBlocks)){
    break()
  }
  minedBlocks <- process_blockstring(minedBlocks)
  blockList[[i]] <- minedBlocks
  if(min(minedBlocks$Height) <= lastKnownBlock){
    break()
  }
  i <- i+1
  Sys.sleep((60/RATE_LIMIT)+1)
}

blockList <- bind_rows(blockList)
blockList <- blockList[blockList$Maturity==0,]
blockList$Pool <- POOL_NAME
blockList$Timestamp <- Sys.time()
blockList$Reward <- as.numeric(blockList$Reward)/10^12
blockList$Date <- as.POSIXct(as.numeric(blockList$Date), origin="1970-01-01", tz="GMT")
blockList$UNK <- NULL
blockList$Maturity <- NULL

blockList <- blockList[blockList$Height>lastKnownBlock,]
if(nrow(blockList)>0){
  blockList <- blockList[,c("Height","Hash", "Date", "Difficulty","Pool","Timestamp","Reward")]
  mysql_fast_db_write_table(con, "block",blockList, append = TRUE)
}

# Step 2: Retrieve remaining payouts
lastKnownPayout <- get_latest_poolpayout(POOL_NAME)
i <- 2
while (TRUE) {
  message(paste("Iteration",i-1))
  payments <- fromJSON(paste0(PAYMENT_ENDPOINT,min(paymentList[[i-1]]$Date)))
  if(length(payments)==0 | "error" %in% names(payments)){
    break()
  }
  payments <- process_paymentstring(payments)
  paymentList[[i]] <- payments
  if(as.numeric(lastKnownPayout)>min(payments$Date)){
    break()
  }
  i <- i+1
  Sys.sleep((60/RATE_LIMIT)+1)
}

paymentList <- bind_rows(paymentList)
paymentList$Pool <- POOL_NAME
paymentList$Timestamp <- Sys.time()
paymentList$Date <- as.POSIXct(as.numeric(paymentList$Date), origin="1970-01-01", tz="GMT")
paymentList$Amount <- as.numeric(paymentList$Amount)/10^12
paymentList$Fee <- as.numeric(paymentList$Fee)/10^12

knownPayoutTx <- get_poolpayouttx(POOL_NAME)
paymentList <- paymentList[!paymentList$TxHash %in% knownPayoutTx$TxHash,]
if(nrow(paymentList)>0){
  paymentList <- paymentList[,c("TxHash", "Amount", "Fee", "Mixin", "Payees", "Date", "Pool", "Timestamp")]
  mysql_fast_db_write_table(con, "payouttransaction",paymentList, append = TRUE)
}
dbDisconnect(con)

