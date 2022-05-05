setwd("/home/jochen/MoneroPoolScraper")
library(jsonlite)
library(DBI)
library(dplyr)
library(R.utils)
library(digest)
source("./DButils.R")
source("./ParseUtils.R")


con <- dbConnect(RMariaDB::MariaDB(), user=DB_USERNAME, password=DB_PW, dbname="monero")

# General Setup
POOL_NAME <- "XMRPool.eu"
POOL_URL <- "https://web.xmrpool.eu/"
POOL_ID <- 4 # For Worker UIDS

# Define API-Endpoints to query
STATS_ENDPOINT <- "https://web.xmrpool.eu:8119/stats"
BLOCKS_ENDPOINT <- "https://web.xmrpool.eu:8119/get_blocks?height="
PAYMENT_ENDPOINT <- "https://web.xmrpool.eu:8119/get_payments?time="
MINER_ENDPOINT <- "https://web.xmrpool.eu:8119/stats_address?address=%s&longpoll=false"
PAYOUT_ENDPOINT <- "https://web.xmrpool.eu:8119/get_payments?time=%i&address=%s"

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
  if(length(minedBlocks)==0){
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

# Step 2: Retrieve remaining payouts
lastKnownPayout <- get_latest_poolpayout(POOL_NAME)
i <- 2
while (TRUE) {
  message(paste("Iteration",i-1))
  payments <- fromJSON(paste0(PAYMENT_ENDPOINT,min(paymentList[[i-1]]$Date)))
  if(length(payments)==0){
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


mysql_fast_db_write_table(con, "block",blockList, append = TRUE)
mysql_fast_db_write_table(con, "payouttransaction",paymentList, append = TRUE)

if(weekdays(Sys.Date()) %in% c("Sunday", "Sonntag")){
  totalMiners <- get_totalminers()
  minerList <- list()
  minersFound <- 1
  
  workerList <- list()
  workersFound <- 1
  
  payoutList <- list()
  payoutsFound <- 1
  for(m in totalMiners$Address){
    minerDetails <- fromJSON(sprintf(MINER_ENDPOINT,m))
    Sys.sleep((60/RATE_LIMIT)+1)
    if(!"error" %in% names(minerDetails) && length(minerDetails) == 3){
      minerList[[minersFound]] <- data.frame(Address = m, Pool = POOL_NAME, 
                                             Balance = as.numeric(as.character(minerDetails[["stats"]][["balance"]]))/10^12,
                                             Timestamp = Sys.time(), stringsAsFactors = FALSE)
      minersFound <- minersFound + 1
      if(length(minerDetails[["perWorkerStats"]])>0){
        workerDetails <- minerDetails[["perWorkerStats"]]
        workerDetails$Miner <- m
        workerDetails$Timestamp <- Sys.time()
        workerList[[workersFound]] <- workerDetails
        workersFound <- workersFound + 1
      }
      if(length(minerDetails[["payments"]])>0){
        minerPayments <- process_paymentstring_xmrpool(minerDetails[["payments"]])
        minerPayments$Miner <- m
        minerPayments$Timestamp <- Sys.time()
        payoutList[[payoutsFound]] <- minerPayments
        payoutsFound <- payoutsFound + 1
        while(TRUE){
          minerPayments <- fromJSON(sprintf(PAYOUT_ENDPOINT, min(as.numeric(as.character(minerPayments$Date))), m))
          Sys.sleep((60/RATE_LIMIT)+1)
          if(length(payoutDetails)==0){
            break()
          }
          minerPayments <- process_paymentstring_xmrpool(minerDetails[["payments"]])
          minerPayments$Miner <- m
          minerPayments$Timestamp <- Sys.time()
          payoutList[[payoutsFound]] <- minerPayments
          payoutsFound <- payoutsFound + 1
        }
      }
    }
  }
  
  minerList <- bind_rows(minerList)
  workerList <- bind_rows(workerList)
  payoutList <- bind_rows(payoutList)
  
  workerList <- workerList[,c("workerId", "lastShare", "Miner", "Timestamp")]
  workerList$Pool <- POOL_NAME
  names(workerList)[c(1,2)] <- c("WorkerID", "LastShare")
  # Fake UID for Workers
  uids <- paste0(workerList$Miner, workerList$WorkerID, POOL_NAME)
  uids <- abs(digest2int(uids))
  if(sum(duplicated(uids))>0) stop("FATAL: Hash collision!")
  uids <- uids + POOL_ID * 10^nchar(uids)
  workerList$UID <- uids
  
  payoutList$Date <- as.POSIXct(as.numeric(payoutList$Date), origin="1970-01-01", tz="GMT")
  payoutList$Amount <- as.numeric(as.character(payoutList$Amount))/10^12
  
  payoutTransactions <- payoutList[,c("TxHash", "Date", "Mixin", "Timestamp")]
  payouts <- payoutList[,c("TxHash", "Miner", "Amount")]
  
  knownMiners <- get_poolminers(POOL_NAME)
  knownWorkers <- get_poolworkers(POOL_NAME)
  knownPayoutTx <- get_poolpayouttx(POOL_NAME)
  knownPayouts <- get_poolpayouts(POOL_NAME)
  
  minerList <- minerList[!minerList$Address %in% knownMiners$Address,]
  workerList <- workerList[!workerList$UID %in% knownWorkers$UID,]
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
