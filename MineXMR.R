setwd("/home/jochen/MoneroPoolScraper")
library(jsonlite)
library(DBI)
library(dplyr)
library(R.utils)
source("./DButils.R")
source("./ParseUtils.R")
con <- dbConnect(RMariaDB::MariaDB(), user=DB_USERNAME, password=DB_PW, dbname="monero")

# General Setup
POOL_NAME <- "MineXMR"
POOL_URL <- "https://minexmr.com"

# Define API-Endpoints to query
BLOCKS_ENDPOINT <- "https://minexmr.com/api/main/pool/blocks/day?"
PAYMENTS_ENDPOINT <- "https://minexmr.com/api/main/user/payments?27524743=&address=%s&page=%i"

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

# Try to find additional information by retrieving info for all known miners (some of them might be active
# on this pool too)

# Takes forever, so only do once a week



if(weekdays(Sys.time()) %in% c("Sunday","Sonntag")){
  paymentList <- list()
  knownMiners <- get_totalminers()
  i <- 1
  for(m in knownMiners$Address){
    if(i%%50==0){
      print(paste("Entry",i))
    }
    j <- 0
    while(TRUE){
      minerPayments <- fromJSON(sprintf(PAYMENTS_ENDPOINT,m,j))
      Sys.sleep((60/RATE_LIMIT)+1)
      if(length(minerPayments)!= 4 || minerPayments[["total"]]==0){
        break()
      }
      payment <- process_paymentstring_minexmr(minerPayments[["payments"]])
      payment$Miner <- m
      payment$Timestamp <- Sys.time()
      paymentList[[i]] <- payment
      i <- i+1
      if(j==minerPayments[["pageCount"]]-1){
        break()
      }
      j <- j+1
    }
    
  }
  
  paymentList <- bind_rows(paymentList)
  
  paymentList$Date <- as.POSIXct(as.numeric(as.character(paymentList$Date)), origin="1970-01-01", tz="GMT")
  paymentList$Amount <- as.numeric(as.character(paymentList$Amount))/10^12
  paymentList$Fee <- as.numeric(as.character(paymentList$Fee))/10^12
  
  
  payoutTransactions <- paymentList[,c("TxHash","Date")]
  payoutTransactions$Pool <- POOL_NAME
  payoutTransactions$Timestamp <- Sys.time()
  payoutTransactions <- payoutTransactions[!duplicated(payoutTransactions),]
  
  knownPayouts <- get_poolpayouts(POOL_NAME)
  paymentList <- paymentList[!paste0(paymentList$Miner, paymentList$TxHash) %in% paste0(knownPayouts$Miner, knownPayouts$TxHash),]
  
  knownPayoutTx <- get_poolpayouttx(POOL_NAME)
  payoutTransactions <- payoutTransactions[!payoutTransactions$TxHash %in% knownPayoutTx$TxHash,]
  
  miners <- paymentList[,c("Miner", "Timestamp")]
  names(miners) <- c("Address", "Timestamp")
  miners <- miners[!duplicated(miners$Address),]
  miners$Pool <- POOL_NAME
  knownMiners <- get_poolminers(POOL_NAME)
  
  miners <- miners[!miners$Address %in% knownMiners$Address,]
  
  if(!dbIsValid(con)){
    con <- dbConnect(RMariaDB::MariaDB(), user=DB_USERNAME, password=DB_PW, dbname="monero")
    
  }
  
  if(nrow(miners)>0){
    mysql_fast_db_write_table(con, "miner",miners, append = TRUE)
  }
  
  
  
  if(nrow(payoutTransactions)>0){
    mysql_fast_db_write_table(con, "payouttransaction",payoutTransactions, append = TRUE)
  }
  
  paymentList <- paymentList[,c("TxHash", "Miner", "Amount", "Timestamp")]
  
  if(nrow(paymentList)>0){
    mysql_fast_db_write_table(con, "payout",paymentList, append = TRUE)
  }
  
}
dbDisconnect(con)
