setwd("/home/jochen/MoneroPoolScraper")
library(jsonlite)
library(DBI)
library(dplyr)
library(R.utils)
source("./DButils.R")

con <- dbConnect(RMariaDB::MariaDB(), user=DB_USERNAME, password=DB_PW, dbname="monero")

# General Setup
POOL_NAME <- "GNTL XMR"
POOL_URL <- "https://xmr.pool.gntl.co.uk"

# Define API-Endpoints to query
BLOCKS_ENDPOINT <- "https://xmr.pool.gntl.co.uk/api/pool/blocks?page=%i&limit=%i"
PAYMENT_ENDPOINT <- "https://xmr.pool.gntl.co.uk/api/pool/payments?page=%i&limit=%i"


# Rate Limit (Max API Hits per Minute)
RATE_LIMIT <- 30

# Preparation: Check if Pool is known to Database (required as Miner references the Pool Name as Foreign Key)
check_pool(POOL_NAME, POOL_URL)

# Preparation: Check for newest known block
lastKnownBlock <- get_latest_poolblock(POOL_NAME)

# Retrieve querySize of the newest Blocks mined by the pool, starting with the one at position (i-1)*queryOffset
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
blockList <- blockList[complete.cases(blockList),]
rm(minedBlocks, i, querySize)

# Subset to blocks that are currently unknown
blockList <- blockList[blockList$height>lastKnownBlock,]
blockList <- blockList[blockList$unlocked & blockList$valid,]

if(nrow(blockList)>0){
  blockList <- blockList[,c("height", "hash", "value","diff","ts")]
  names(blockList) <- c("Height", "Hash", "Reward", "Difficulty", "Date")
  blockList$Pool <- POOL_NAME
  blockList$Miner <- NA
  blockList$Date <- as.POSIXct(blockList$Date/1000, origin="1970-01-01", tz="GMT")
  # To Monero Base-Unit
  blockList$Reward <- blockList$Reward/10^12
  blockList$Timestamp <- Sys.time()
  
  mysql_fast_db_write_table(con, "block",blockList, append = TRUE)
}

# Preparation: Check for newest known block
lastKnownPayout <- get_latest_poolpayout(POOL_NAME)

# Retrieve querySize of the newest payouts made by the pool, starting with the one at position (i-1)*queryOffset
paymentList <- list()
i <- 1
querySize <- 1000
while(TRUE){
  message(paste("Iteration",i))
  payment <- fromJSON(sprintf(PAYMENT_ENDPOINT, i-1, querySize))
  payment$ts <- as.POSIXct(payment$ts/1000, origin="1970-01-01", tz="GMT")
  paymentList[[i]] <- payment
  i <- i+1
  # Break if less than querySize Blocks were retrieved, i.e. we have reached the maximum amount of data
  # or if we know the blocks already
  if(nrow(payment)<querySize|min(payment$ts) <= lastKnownPayout){
    break()
  }
  # Rate Limiter
  Sys.sleep((60/RATE_LIMIT)+1)
}
paymentList <- bind_rows(paymentList)
paymentList <- paymentList[complete.cases(paymentList),]
rm(payment, i)

paymentList <- paymentList[paymentList$ts >= lastKnownPayout,]

if(nrow(paymentList)>0){
  paymentList <- paymentList[,c("hash","ts","fee","payees","value","mixins")]
  paymentList$Pool <- POOL_NAME
  paymentList$value <- paymentList$value/10^12
  paymentList$fee <- paymentList$fee/10^12
  names(paymentList) <- c("TxHash","Date","Fee","Payees","Amount","Mixin","Pool")
  paymentList$Timestamp <- Sys.time()
  mysql_fast_db_write_table(con, "payouttransaction",paymentList, append = TRUE)
}

dbDisconnect(con)
