setwd("/home/jochen/MoneroPoolScraper")
library(jsonlite)
library(DBI)
library(dplyr)
library(R.utils)
source("./DButils.R")

con <- dbConnect(RMariaDB::MariaDB(), user=DB_USERNAME, password=DB_PW, dbname="monero", port=3307)

# General Setup
POOL_NAME <- "Nanopool"
POOL_URL <- "https://xmr.nanopool.org/"

# Define API-Endpoints to query
BLOCKS_ENDPOINT <- "https://xmr.nanopool.org/api/v1/pool/blocks/"
PAYMENT_ENDPOINT <- "https://api.nanopool.org/v1/xmr/payments/"
TOPMINER_ENDPOINT <- "https://api.nanopool.org/v1/xmr/pool/topminers"
MINERDETAILs_ENDPOINT <- "https://api.nanopool.org/v1/xmr/user/"
HASHHISTORY_ENDPOINT <- "https://api.nanopool.org/v1/xmr/history/"
SHAREHISTORY_ENDPOINT <- "https://api.nanopool.org/v1/xmr/shareratehistory/"

# Rate Limit (Max API Hits per Minute)
RATE_LIMIT <- 30

# Preparation: Check if Pool is known to Database (required as Miner references the Pool Name as Foreign Key)
check_pool(POOL_NAME, POOL_URL)

# Preparation: Check for newest known block
lastKnownBlock <- get_latest_poolblock(POOL_NAME)

# Step 1: Generate List of Miners by retrieving Blocks:

# Retrieve querySize of the newest Blocks mined by the pool, starting with the one at position queryOffset
blockList <- list()
i <- 1
queryOffset <- 0
querySize <- 1000
while(TRUE){
  message(paste("Iteration",i))
  minedBlocks <- fromJSON(paste0(BLOCKS_ENDPOINT,queryOffset,"/",querySize))
  minedBlocks <- minedBlocks["data"][[1]]
  blockList[[i]] <- minedBlocks
  i <- i+1
  # Break if less than querySize Blocks were retrieved, i.e. we have reached the maximum amount of data
  # or if we know the blocks already
  if(nrow(minedBlocks)<querySize | min(minedBlocks$block_number)<=lastKnownBlock){
    break()
  }
  queryOffset <- queryOffset + querySize
  # Rate Limiter
  Sys.sleep((60/RATE_LIMIT)+1)
}
blockList <- bind_rows(blockList)
blockList <- blockList[complete.cases(blockList),]
rm(minedBlocks, i, queryOffset, querySize)

# Subset to blocks that are currently unknown
blockList <- blockList[blockList$block_number>lastKnownBlock,]

# Step 2: Retrieve Top Miners

topMiners <- fromJSON(TOPMINER_ENDPOINT)
topMiners <- topMiners["data"][[1]]

# Step 3: Combine Miners found in the Previous Steps and deduplicate

miners <- unique(c(blockList$miner, topMiners$address))
message(paste("Found",length(miners),"distinct miners."))
rm(topMiners, lastKnownBlock)

# Check which of those Miners are already known
rows <- get_poolminers(POOL_NAME)
miners <- miners[!miners %in% rows$Address]

# Step 4: Retrieve Miner Details:
if(length(miners)>0){
  minerList <- list()
  workerList <- list()
  i <- 1
  pb <- txtProgressBar(max=length(miners), style = 3)
  for(m in miners){
    minerDetails <- fromJSON(paste0(MINERDETAILs_ENDPOINT,m))
    minerDetails <- minerDetails["data"][[1]]
    minerWorkers <- minerDetails["workers"][[1]]
    minerDetails <- data.frame(minerDetails[1:5])
    if(length(minerDetails)==0){
      next()
    }
    minerDetails$Pool <- POOL_NAME
    minerDetails$Timestamp <- Sys.time()
    if(length(minerWorkers)>0){
      minerWorkers$Pool <- POOL_NAME
      minerWorkers$Miner <- m
      minerWorkers$Timestamp <- Sys.time()
      workerList[[i]] <- minerWorkers
    }
    minerList[[i]] <- minerDetails
    # Rate Limiter
    Sys.sleep((60/RATE_LIMIT)+1)
    setTxtProgressBar(pb, i)
    i <- i+1
  }
  
  minerList <- bind_rows(minerList)
  names(minerList)[1:9] <- c("Address","UncoBalance","Balance","Hashrate","H1","H3","H6","H12","H24")
  workerList <- bind_rows(workerList)
  names(workerList)[1:10] <- c("WorkerID","UID","Hashrate","LastShare","Rating","H1","H3","H6","H12","H24")
  workerList$LastShare <- as.POSIXct(workerList$LastShare, origin="1970-01-01", tz="GMT")
  
  # Batch-Write retrieved Data to Database
  mysql_fast_db_write_table(con, "miner",minerList, append = TRUE)
  mysql_fast_db_write_table(con, "worker",workerList, append = TRUE)
  
  # Step 5: Retrieve Historic Hashrates
  rm(pb, i, m, minerDetails, minerWorkers)
  
  hashList <- list()
  i <- 1
  pb <- txtProgressBar(max=length(miners), style = 3)
  for(m in miners){
    hashHistory <- fromJSON(paste0(HASHHISTORY_ENDPOINT,m))
    hashHistory <- hashHistory["data"][[1]]
    if(length(hashHistory)>0){
      hashHistory$date <- as.POSIXct(hashHistory$date, origin="1970-01-01", tz="GMT")
      hashHistory$Miner <- m
      hashHistory$Timestamp <- Sys.time()
    }
    hashList[[i]] <- hashHistory
    # Rate Limiter
    Sys.sleep((60/RATE_LIMIT)+1)
    setTxtProgressBar(pb, i)
    i <- i+1
  }
  
  hashList <- bind_rows(hashList)
  names(hashList)[1:2] <- c("RefTime", "Hashrate")
  mysql_fast_db_write_table(con, "hashrate",hashList, append = TRUE)
  
  
  # Step 5: Retrieve Historic Sharerates
  rm(pb, i, m)
  
  shareList <- list()
  i <- 1
  pb <- txtProgressBar(max=length(miners), style = 3)
  for(m in miners){
    shareHistory <- fromJSON(paste0(SHAREHISTORY_ENDPOINT,m))
    shareHistory <- shareHistory["data"][[1]]
    if(length(shareHistory)>0){
      shareHistory$Miner <- m
      shareHistory$Timestamp <- Sys.time()
      shareList[[i]] <- shareHistory
    }
    # Rate Limiter
    Sys.sleep((60/RATE_LIMIT)+1)
    setTxtProgressBar(pb, i)
    i <- i+1
  }
  
  
  shareList <- bind_rows(shareList)
  shareList$date <- as.POSIXct(shareList$date, origin="1970-01-01", tz = "GMT")
  names(shareList)[1:2] <- c("RefTime", "Shares")
  mysql_fast_db_write_table(con, "sharerate",shareList, append = TRUE)
  rm(pb, i, m)
}
# Step 6: Retrieve Payouts

# Payments should be updated for all miners. Thus retrieve all known miners from the database
rows <- get_poolminers(POOL_NAME)
miners <- rows$Address
rm(res, rows)

paymentList <- list()
i <- 1
pb <- txtProgressBar(max=length(miners), style = 3)
for(m in miners){
  payments <- fromJSON(paste0(PAYMENT_ENDPOINT,m))
  payments <- payments["data"][[1]]
  if(length(payments)>0){
    payments$Miner <- m
    payments$Timestamp <- Sys.time()
    paymentList[[i]] <- payments
  }
  # Rate Limiter
  Sys.sleep((60/RATE_LIMIT)+1)
  setTxtProgressBar(pb, i)
  i <- i+1
}

paymentList <- bind_rows(paymentList)

payout <- paymentList[,c("txHash","Miner","amount","Timestamp")]
names(payout)[c(1,3)] <- c("TxHash","Amount")

payoutTx <- paymentList[,c("txHash", "Timestamp")]
names(payoutTx)[1] <- "TxHash"
payoutTx$Pool <- POOL_NAME
payoutTx$Fee <- NA
payoutTx$Date <- NA
payoutTx <- payoutTx[!duplicated(payoutTx$TxHash),]

knownPayoutTx <- get_poolpayouttx(POOL_NAME)

payout <- payout[!payout$TxHash %in% knownPayoutTx$TxHash,]
payoutTx <- payoutTx[!payoutTx$TxHash %in% knownPayoutTx$TxHash,]

# Payout references PayoutTX as Foreign Key -> Must be written first

mysql_fast_db_write_table(con, "payouttransaction",payoutTx, append = TRUE)
mysql_fast_db_write_table(con, "payout",payout, append = TRUE)

# Final Step: Insert Blocks
blockList$date <- as.POSIXct(blockList$date, origin="1970-01-01", tz = "GMT")
blockList$status <- NULL
names(blockList) <- c("Height","Hash","Date","Reward","Miner")
blockList$Pool <- POOL_NAME
blockList$Timestamp <- Sys.time()
mysql_fast_db_write_table(con, "block",blockList, append = TRUE)

dbDisconnect(con)

