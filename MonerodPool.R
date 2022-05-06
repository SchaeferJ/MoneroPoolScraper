setwd("/home/jochen/MoneroPoolScraper")
library(jsonlite)
library(DBI)
library(dplyr)
library(R.utils)
library(digest)
source("./DButils.R")

con <- dbConnect(RMariaDB::MariaDB(), user=DB_USERNAME, password=DB_PW, dbname="monero")
# General Setup
POOL_NAME <- "Monerod.org"
POOL_URL <- "https://monerod.org"
POOL_ID <- 7

# Preparation: Check if Pool is known to Database (required as Miner references the Pool Name as Foreign Key)
check_pool(POOL_NAME, POOL_URL)

# Define API-Endpoints to query
BLOCKS_ENDPOINT <- "https://np-api.monerod.org/pool/blocks?page=%i&limit=%i"
PAYMENT_ENDPOINT <- "https://np-api.monerod.org/pool/payments?page=%i&limit=%i"
MINER_ENDPOINT <- "https://np-api.monerod.org/miner/%s/stats"
PAYMENT_ENDPOINT <- "https://np-api.monerod.org/miner/%s/payments"
WORKER_ENDPOINT <- "https://np-api.monerod.org/miner/%s/stats/allworkers"
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
while(TRUE){
  message(paste("Iteration",i))
  
  payments <- fromJSON(sprintf(PAYMENT_ENDPOINT, i-1, querySize))
  if(length(payments)==0){
    break()
  }
  payments$ts <- as.POSIXct(as.numeric(payments$ts)/1000, origin="1970-01-01", tz="GMT")
  
  paymentList[[i]] <- payments
  i <- i+1
  # Break if less than querySize Blocks were retrieved, i.e. we have reached the maximum amount of data
  # or if we know the blocks already
  if(nrow(payments)<querySize | min(payments$ts)<=lastKnownPayout){
    break()
  }
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

if(weekdays(Sys.Date()) %in% c("Sunday","Sonntag")){
  totalMiners <- get_totalminers()
  minerList <- list()
  minersFound <- 1
  
  paymentList <- list()
  paymentsFound <- 1
  
  workerList <- list()
  workersFound <- 1
  
  for (m in totalMiners$Address) {
    minerDetails <- fromJSON(sprintf(MINER_ENDPOINT, m))
    Sys.sleep((60/RATE_LIMIT)+1)
    if(!is.null(minerDetails[["lastHash"]])){
      minerDetails <- data.frame(minerDetails, stringsAsFactors = FALSE)
      minerDetails$Address <- m
      minerDetails$Timestamp <- Sys.time()
      minerList[[minersFound]] <- minerDetails
      minersFound <- minersFound + 1
      minerPayments <- fromJSON(sprintf(PAYMENT_ENDPOINT, m))
      Sys.sleep((60/RATE_LIMIT)+1)
      if(length(minerPayments)>0){
        minerPayments$Miner <- m
        minerPayments$Timestamp <- Sys.time()
        paymentList[[paymentsFound]] <- minerPayments
        paymentsFound <- paymentsFound + 1
      }
      workers <- fromJSON(sprintf(WORKER_ENDPOINT,m))
      for (w in workers) {
        w <- data.frame(w, stringsAsFactors = FALSE)
        w$Miner <- m
        w$Timestamp <- Sys.time()
        workerList[[workersFound]] <- w
        workersFound <- workersFound + 1
      }
    }
  }
  
  minerList <- bind_rows(minerList)
  paymentList <- bind_rows(paymentList)
  workerList <- bind_rows(workerList)
  
  minerList$amtPaid <- minerList$amtPaid/10^12
  minerList$amtDue <- minerList$amtDue/10^12
  minerList$Pool <- POOL_NAME
  minerList <- minerList[,c("Address", "Pool", "amtDue", "amtPaid", "Timestamp")]
  names(minerList)[c(3,4)] <- c("Balance", "TotalPayout")
  
  paymentList$ts <- as.POSIXct(as.numeric(as.character(paymentList$ts)), origin="1970-01-01", tz="GMT")
  paymentList$amount <- paymentList$amount/10^12
  names(paymentList)[c(1:5)] <- c("pt", "Date", "Amount", "TxHash", "Mixin")
  
  payoutTransactions <- paymentList[,c("TxHash", "Date", "Mixin", "Timestamp")]
  payoutTransactions$Pool <- POOL_NAME
  
  payouts <- paymentList[,c("TxHash","Miner","Amount","Timestamp")]
  

  if(nrow(workerList)>0){
    
    workerList$Pool <- POOL_NAME
    workerList <- workerList[,c("identifer", "Miner", "Pool", "lts", "hash", "Timestamp")]
    names(workerList) <- c("WorkerID", "Miner", "Pool", "LastShare", "H1", "Timestamp")
    workerList$LastShare <- as.POSIXct(as.numeric(as.character(workerList$LastShare)), origin="1970-01-01", tz="GMT")
    
    # Fake UID for Workers
    uids <- paste0(workerList$Miner, workerList$WorkerID, POOL_NAME)
    uids <- abs(digest2int(uids))
    if(sum(duplicated(uids))>0) stop("FATAL: Hash collision!")
    uids <- uids + POOL_ID * 10^nchar(uids)
    workerList$UID <- uids
    
    knownWorkers <- get_poolworkers(POOL_NAME)
    workerList <- workerList[!workerList$UID %in% knownWorkers$UID,]
    
  }
  
  knownMiners <- get_poolminers(POOL_NAME)
  knownPayoutTx <- get_poolpayouttx(POOL_NAME)
  knownPayouts <- get_poolpayouts(POOL_NAME)
  
  minerList <- minerList[!minerList$Address %in% knownMiners$Address,]
  payoutTransactions <- payoutTransactions[!payoutTransactions$TxHash %in% knownPayoutTx$TxHash]
  payouts <- payouts[!paste0(payouts$TxHash,payouts$Miner) %in% paste0(knownPayouts$TxHash, knownPayouts$Miner)]
  
  if(nrow(minerList)>0){
    mysql_fast_db_write_table(con, "miner",minerList, append = TRUE)
  }
  
  if(nrow(workerList)>0){
    mysql_fast_db_write_table(con, "worker",workerList, append = TRUE)
  }
  
  if(nrow(payoutTransactions)>0){
    mysql_fast_db_write_table(con, "payouttransaction",payoutTransactions, append = TRUE)
  }
  
  if(nrow(payouts)>0){
    mysql_fast_db_write_table(con, "payout",minerList, append = TRUE)
  }
  
}


dbDisconnect(con)
