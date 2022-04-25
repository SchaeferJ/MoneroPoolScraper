setwd("/home/jochen/MoneroPoolScraper")
library(jsonlite)
library(DBI)
library(dplyr)
library(R.utils)
source("./DButils.R")

con <- dbConnect(RMariaDB::MariaDB(), user=DB_USERNAME, password=DB_PW, dbname="monero")

# General Setup
POOL_NAME <- "Sargatxet"
POOL_URL <- "https://xmrpool.sargatxet.cloud/"

# Define API-Endpoints to query
BLOCKS_ENDPOINT <- "https://xmrpool.sargatxet.cloud/api/pool/blocks?page=%i&limit=%i"
PAYMENT_ENDPOINT <- "https://xmrpool.sargatxet.cloud/api/pool/payments?page=%i&limit=%i"


# Rate Limit (Max API Hits per Minute)
RATE_LIMIT <- 30

# Preparation: Check if Pool is known to Database (required as Miner references the Pool Name as Foreign Key)
check_pool(POOL_NAME, POOL_URL)

# Preparation: Check for newest known block
lastKnownBlock <- get_latest_poolblock(POOL_NAME)

# Step 1: Generate List of Miners by retrieving Blocks:

# Retrieve querySize of the newest Blocks mined by the pool, starting with the one at position i-1*querySize
blockList <- list()
i <- 1
querySize <- 1000
while(TRUE){
  message(paste("Iteration",i))
  
  minedBlocks <- fromJSON(sprintf(BLOCKS_ENDPOINT, i-1, querySize))
  blockList[[i]] <- minedBlocks
  i <- i+1
  # Break if less than querySize Blocks were retrieved, i.e. we have reached the maximum amount of data
  # or if we know the blocks already
  if(nrow(minedBlocks)<querySize | min(minedBlocks$height)<=lastKnownBlock){
    break()
  }
  # Rate Limiter
  Sys.sleep((60/RATE_LIMIT)+1)
}

blockList <- bind_rows(blockList)
rm(minedBlocks, i, querySize)

# Subset to blocks that are currently unknown
blockList <- blockList[blockList$height>lastKnownBlock,]
blockList <- blockList[blockList$unlocked&blockList$valid,]

if(nrow(blockList)>0){
  blockList$ts <- as.POSIXct(as.numeric(blockList$ts)/1000, origin="1970-01-01", tz="GMT")
  blockList$value <- as.numeric(blockList$value)/10^12
  blockList$shares <- NULL
  blockList$unlocked <- NULL
  blockList$pool_type <- NULL
  blockList$finder <- NULL
  blockList$valid <- NULL
  
  names(blockList) <- c("Date", "Hash", "Difficulty", "Height", "Reward")
  blockList$Timestamp <- Sys.time()
  blockList$Pool <- POOL_NAME
  mysql_fast_db_write_table(con, "block",blockList, append = TRUE)
}


# Step 2: Retrieve Payments
lastKnownPayout <- get_latest_poolpayout(POOL_NAME)
paymentList <- list()
i <- 1
querySize <- 1000
queryOffset <- 0
while(TRUE){
  message(paste("Iteration",i))
  
  payments <- fromJSON(sprintf(PAYMENT_ENDPOINT, queryOffset, querySize))
  payments$ts <- as.POSIXct(as.numeric(payments$ts)/1000, origin="1970-01-01", tz="GMT")
  paymentList[[i]] <- payments
  i <- i+1
  # Break if less than querySize Blocks were retrieved, i.e. we have reached the maximum amount of data
  # or if we know the blocks already
  if(nrow(payments)<querySize | min(payments$ts)<=lastKnownPayout){
    break()
  }
  queryOffset <- queryOffset + querySize
  # Rate Limiter
  Sys.sleep((60/RATE_LIMIT)+1)
}

paymentList <- bind_rows(paymentList)
paymentList$value <- paymentList$value/10^12

paymentList$pool_type <- NULL
paymentList$id <- NULL
paymentList$fee <- paymentList$fee/10^12
names(paymentList) <- c("TxHash", "Mixin", "Payees", "Fee", "Amount", "Date")
paymentList$Pool <- POOL_NAME
paymentList$Timestamp <- Sys.time()

knownPayoutTx <- get_poolpayouttx(POOL_NAME)
paymentList <- paymentList[!paymentList$TxHash %in% knownPayoutTx$TxHash,]

if(nrow(paymentList)>0){
  mysql_fast_db_write_table(con, "payouttransaction",paymentList, append = TRUE)
}
dbDisconnect(con)
