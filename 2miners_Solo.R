setwd("/home/jochen/MoneroPoolScraper")
library(jsonlite)
library(DBI)
library(dplyr)
library(R.utils)
library(htmltab)
library(digest)
source("./DButils.R")

con <- dbConnect(RMariaDB::MariaDB(), user=DB_USERNAME, password=DB_PW, dbname="monero")

# General Setup
POOL_NAME <- "2miners SOLO"
POOL_URL <- "https://solo-xmr.2miners.com/"
POOL_ID <- 2 # For Worker UIDS

# Define API-Endpoints to query
BLOCKS_ENDPOINT <- "https://solo-xmr.2miners.com/api/blocks"
ACTIVEMINER_ENDPOINT <- "https://solo-xmr.2miners.com/api/miners"
MINERDETAILS_ENDPOINT <- "https://solo-xmr.2miners.com/api/accounts/"
PAYMENT_ENDPOINT <- "https://solo-xmr.2miners.com/api/payments"

# Rate Limit (Max API Hits per Minute)
RATE_LIMIT <- 30

# Preparation: Check if Pool is known to Database (required as Miner references the Pool Name as Foreign Key)
check_pool(POOL_NAME, POOL_URL)

# Preparation: Check for newest known block
lastKnownBlock <- get_latest_poolblock(POOL_NAME)

# Step 1: Generate List of Miners by retrieving Blocks:
minedBlocks <- fromJSON(BLOCKS_ENDPOINT)
blockList <- minedBlocks[["matured"]]

blockList <- blockList[complete.cases(blockList),]
rm(minedBlocks)

# Subset to blocks that are currently unknown
blockList <- blockList[blockList$height>lastKnownBlock,]


# Step 2: Retrieve active Miners

activeMiners <- fromJSON(ACTIVEMINER_ENDPOINT)
activeMiners <- activeMiners[["miners"]]
miners <- names(activeMiners)
activeMiners <- bind_rows(activeMiners)
activeMiners$miner <- miners

# Step 3: Combine Miners found in the Previous Steps and deduplicate
miners <- unique(c(blockList$finder, miners))
message(paste("Found",length(miners),"distinct miners."))


# Add Miners that are already known
miners <- unique(c(get_poolminers(POOL_NAME)$Address, miners))


# Step 4: Retrieve Miner Details:

minerList <- list()
workerList <- list()
hashList <- list()
paymentList <- list()
i <- 1
pb <- txtProgressBar(max=length(miners), style = 3)
for(m in miners){
  minerDetails <- fromJSON(paste0(MINERDETAILS_ENDPOINT,m))
  
  
  minerHashes <- minerDetails[["minerCharts"]]
  if(length(minerHashes)>0){
    minerHashes$miner <- m
    minerHashes$Timestamp <- Sys.time()
    hashList[[i]] <- minerHashes
  }
  
  minerWorkers <- minerDetails[["workers"]]
  if(length(minerWorkers)>0){
    workers <- names(minerWorkers)
    minerWorkers <- bind_rows(minerWorkers)
    minerWorkers$workerID <- workers
    minerWorkers$miner <- m
    minerWorkers$Timestamp <- Sys.time()
    workerList[[i]] <- minerWorkers
  }
  
  minerPayments <- minerDetails[["payments"]]
  if(length(minerPayments)>0){
    minerPayments$Miner <- m
    minerPayments$timestamp <- Sys.time()
    paymentList[[i]] <- minerPayments
  }
  
  TotalPayout <- minerDetails[["stats"]][["paid"]]
  if(is.null(TotalPayout)) TotalPayout <- 0
  
  UncoBalance <- minerDetails[["stats"]][["pending"]]
  if(is.null(UncoBalance)) UncoBalance <- 0
  
  Balance <- minerDetails[["stats"]][["balance"]]
  if(is.null(Balance)) Balance <- 0
  
  Hashrate <- minerDetails[["currentHashrate"]]
  if(is.null(Hashrate)) Hashrate <- 0
  
  H1 <- minerDetails[["hashrate"]]
  if(is.null(H1)) H1 <- 0
  
  BlocksFound <- minerDetails[["stats"]][["blocksFound"]]
  if(is.null(BlocksFound)) BlocksFound <- 0
  
  minerList[[i]] <- data.frame(Address=m, Pool=POOL_NAME, Balance,
                     UncoBalance, Hashrate,
                     H1, TotalPayout,
                     BlocksFound)
  
  # Rate Limiter
  Sys.sleep((60/RATE_LIMIT)+1)
  setTxtProgressBar(pb, i)
  i <- i+1
}

minerList <- bind_rows(minerList)
workerList <- bind_rows(workerList)
hashList <- bind_rows(hashList)
paymentList <- bind_rows(paymentList)

minerList$TotalPayout <- minerList$TotalPayout/10^12

workerList$offline <- NULL
names(workerList) <- c("LastShare", "Hashrate", "H1", "WorkerID", "Miner", "Timestamp" )
workerList$Pool <- POOL_NAME
workerList$LastShare <- as.POSIXct(workerList$LastShare, origin="1970-01-01", tz="GMT")
# Fake UID for Workers
uids <- paste0(workerList$Miner, workerList$WorkerID, POOL_NAME)
uids <- abs(digest2int(uids))
if(sum(duplicated(uids))>0) stop("FATAL: Hash collision!")
uids <- uids + POOL_ID * 10^nchar(uids)
workerList$UID <- uids


hashList$x <- as.POSIXct(hashList$x, origin="1970-01-01", tz="GMT")
hashList$timeFormat <- NULL
hashList$minerLargeHash <- NULL
names(hashList) <- c("Hashrate", "WorkersOnline", "RefTime", "Miner", "Timestamp")

payoutTransactions <- data.frame(TxHash = unique(paymentList$tx), Pool = POOL_NAME, Timestamp = Sys.time())

names(paymentList) <- c("Amount", "Timestamp", "TxHash", "Miner")
paymentList$Amount <- paymentList$Amount/10^12

blockList$shares <- NULL
blockList$orphan <- NULL
blockList$currentLuck <- NULL
blockList$timestamp <- as.POSIXct(blockList$timestamp, origin="1970-01-01", tz="GMT")
blockList$reward <- blockList$reward/10^12
names(blockList) <- c("Height", "Date", "Difficulty", "Miner", "Hash", "Reward")
blockList$Timestamp <- Sys.time()
blockList$Pool <- POOL_NAME


# Step 5: Retrieve Payouts
payouts <- fromJSON(PAYMENT_ENDPOINT)
payouts <- payouts[["payments"]]
payouts$timestamp <- NULL
payouts$amount <- NULL
names(payouts) <- c("Payees", "TxHash")
payouts$Pool <- POOL_NAME
payouts$Timestamp <- Sys.time()

payoutTransactions$Payees <- NA
payoutTransactions <- rbind(payoutTransactions[!payoutTransactions$TxHash %in% payouts$TxHash,], payouts)

# Remove Data that is already known
knownMiners <- get_poolminers(POOL_NAME)
knownWorkers <- get_poolworkers(POOL_NAME)
knownHashes <- get_poolhashes(POOL_NAME)
knownPayoutTx <- get_poolpayouttx(POOL_NAME)
knownPayouts <- get_poolpayouts(POOL_NAME)

minerList <- minerList[!minerList$Address %in% knownMiners$Address,]
workerList <- workerList[!workerList$UID %in% as.numeric(knownWorkers$UID),]
hashList <- hashList[!paste0(hashList$RefTime, hashList$Miner) %in% paste0(knownHashes$RefTime, knownHashes$Miner),]
payoutTransactions <- payoutTransactions[!payoutTransactions$TxHash %in% knownPayoutTx$TxHash,]
paymentList <- paymentList[!paste0(paymentList$TxHash, paymentList$Miner) %in% paste0(knownPayouts$TxHash, knownPayouts$Miner),]

# Batch-Write retrieved Data to Database
mysql_fast_db_write_table(con, "miner",minerList, append = TRUE)
mysql_fast_db_write_table(con, "worker",workerList, append = TRUE)
mysql_fast_db_write_table(con, "hashrate",hashList, append = TRUE)
mysql_fast_db_write_table(con, "payouttransaction",payoutTransactions, append = TRUE)
mysql_fast_db_write_table(con, "payout",paymentList, append = TRUE)
mysql_fast_db_write_table(con, "block",blockList, append = TRUE)
dbDisconnect(con)

