setwd("/home/jochen/MoneroPoolScraper")
library(jsonlite)
library(DBI)
library(dplyr)
library(R.utils)
library(digest)
source("./DButils.R")

con <- dbConnect(RMariaDB::MariaDB(), user=DB_USERNAME, password=DB_PW, dbname="monero")

# General Setup
POOL_NAME <- "HashVault"
POOL_URL <- "https://www.hashvault.pro/"

# Define API-Endpoints to query
BLOCKS_ENDPOINT <- "https://api.hashvault.pro/v3/monero/pool/blocks?limit=%i&page=%i&pooltype=collective"
PAYMENT_ENDPOINT <- "https://api.hashvault.pro/v3/monero/pool/payments?limit=%i&page=%i"


# Rate Limit (Max API Hits per Minute)
RATE_LIMIT <- 30

# Preparation: Check if Pool is known to Database (required as Miner references the Pool Name as Foreign Key)
check_pool(POOL_NAME, POOL_URL)

# Preparation: Check for newest known block
lastKnownBlock <- get_latest_poolblock(POOL_NAME)

# Steo 1: Retrieve Blocks

querySize <- 1000
i <- 1
blockList <- list()
while(TRUE){
  message(paste("Iteration",i))
  minedBlocks <- fromJSON(sprintf(BLOCKS_ENDPOINT,querySize, i-1))
  blockList[[i]] <- minedBlocks
  
  if(nrow(minedBlocks)<querySize | min(minedBlocks$height)<=lastKnownBlock){
    break()
  }
  
  i <- i+1
  Sys.sleep((60/RATE_LIMIT)+1)
}

blockList <- bind_rows(blockList)
blockList$ts <- as.POSIXct(blockList$ts/1000, origin="1970-01-01", tz="GMT")
blockList$value <- blockList$value/10^12
blockList <- blockList[blockList$valid & blockList$credited,]
blockList <- blockList[,c("height", "foundBy", "hash", "ts", "value", "diff")]
names(blockList) <- c("Height", "MinerHash", "Hash", "Date", "Reward", "Difficulty")
blockList <- blockList[blockList$Height > lastKnownBlock,]
if(nrow(blockList)>0){
  blockList$Timestamp <- Sys.time()
  blockList$Pool <- POOL_NAME
}
lastKnownPayout <- get_latest_poolpayout(POOL_NAME)

querySize <- 1000
i <- 1
paymentList <- list()
while(TRUE){
  message(paste("Iteration",i))
  payments <- fromJSON(sprintf(PAYMENT_ENDPOINT,querySize, i-1))
  if(length(payments)==0){
    break()
  }
  payments$ts <- as.POSIXct(payments$ts/1000, origin="1970-01-01", tz="GMT")
  paymentList[[i]] <- payments
  
  if(nrow(payments)<querySize | min(payments$ts)<=lastKnownPayout){
    break()
  }
  
  i <- i+1
  Sys.sleep((60/RATE_LIMIT)+1)
}

paymentList <- bind_rows(paymentList)
paymentList <- paymentList[paymentList$ts >= lastKnownPayout,]
if(nrow(paymentList)>0){
  paymentList$value <- paymentList$value/10^12
  paymentList$fee <- paymentList$fee/10^12
  paymentList$id <- NULL
  names(paymentList) <- c("TxHash", "Mixin", "Payees", "Fee", "Amount", "Date")
  paymentList$Timestamp <- Sys.time()
  paymentList$Pool <- POOL_NAME
  mysql_fast_db_write_table(con, "payouttransaction",paymentList, append = TRUE)
}
# Try to Determine Block Miners
if(nrow(blockList)>0){
  knownMiners <- get_totalminers()
  knownMiners$MinerHash <- sapply(knownMiners$Address, function (x) digest(x, algo="md5")) 
  
  blockList <- merge(blockList, knownMiners, by="MinerHash", all.x = TRUE)
  names(blockList)[ncol(blockList)] <- "Miner"
  
  mysql_fast_db_write_table(con, "block",blockList, append = TRUE)
}

dbDisconnect(con)
