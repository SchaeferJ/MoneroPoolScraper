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



if(weekdays(Sys.Date()) %in% c("Sunday", "Sonntag")){
  totalMiners <- get_totalminers()
  minerList <- list()
  payoutList <- list()
  minersFound <- 1
  payoutsFound <- 1
  
  for(m in totalMiners$Address){
    minerDetails <- fromJSON(sprintf(MINER_ENDPOINT, m))
    Sys.sleep((RATE_LIMIT/60)+1)
    if("error" %in% names(minerDetails)){
      next()
    }
    if(minerDetails[["collective"]][["lastShare"]] != 0 | minerDetails[["solo"]][["lastShare"]] != 0){
      
      minerList[[minersFound]] <- data.frame(Address=m, Pool = POOL_NAME, Balance = minerDetails[["revenue"]][["confirmedBalance"]],
                                             UncoBalance = sum(minerDetails[["revenue"]][["unconfirmedBalance"]][["collective"]][["total"]],
                                                               minerDetails[["revenue"]][["unconfirmedBalance"]][["solo"]][["total"]]),
                                             TotalPayout = minerDetails[["revenue"]][["totalPaid"]],
                                             BlocksFound = sum(minerDetails[["collective"]][["foundBlocks"]], minerDetails[["solo"]][["foundBlocks"]]),
                                             Timestamp = Sys.time(), stringsAsFactors = FALSE)
      
      minersFound <- minersFound + 1
      Sys.sleep((60/RATE_LIMIT)+1)
      
      if(minerDetails[["revenue"]][["totalPaymentsSent"]]>0){
        page <- 0
        while (TRUE) {
          minerPayouts <- fromJSON(sprintf(PAYOUT_ENDPOINT,m,page))
          if(length(minerPayouts) == 0 || nrow(minerPayouts)==0){
            break()
          }
          minerPayouts$Miner <- m
          minerPayouts$Timestamp <- Sys.time()
          payoutList[[payoutsFound]] <- minerPayouts
          payoutsFound <- payoutsFound + 1
          page <- page + 1
          Sys.sleep((60/RATE_LIMIT)+1)
          
        }
        
      }
    }
  }
  
  minerList <- bind_rows(minerList)
  payoutList <- bind_rows(payoutList)
  
  minerList$Balance <- as.numeric(as.character(minerList$Balance))/10^12
  minerList$UncoBalance <- as.numeric(as.character(minerList$UncoBalance))/10^12
  minerList$TotalPayout <- as.numeric(as.character(minerList$TotalPayout))/10^12
  
  payoutList$ts <- as.POSIXct(payoutList$ts, origin="1970-01-01", tz="GMT")
  payoutList$amount <- as.numeric(as.character(payoutList$amount))/10^12
  payoutList$paidFee <- as.numeric(as.character(payoutList$paidFee))/10^12
  
  names(payoutList) <- c("Date", "Amount", "Fee", "TxHash", "TransactionKey", "Mixin", "Miner", "Timestamp")
  
  payoutTransactions <- payoutList[,c("TxHash", "Date", "Mixin", "TransactionKey", "Timestamp")]
  payoutTransactions$Pool <- POOL_NAME
  payoutTransactions <- payoutTransactions[!duplicated(payoutTransactions$TxHash),]
  
  payoutList <- payoutList[,c("TxHash", "Miner", "Amount")]
  
  knownMiners <- get_poolminers(POOL_NAME)
  knownPayoutTxs <- get_poolpayouttx(POOL_NAME)
  knownPayouts <- get_poolpayouts(POOL_NAME)
  
  minerList <- minerList[!minerList$Address %in% knownMiners$Address, ]
  payoutTransactions <- payoutTransactions[!payoutTransactions$TxHash %in% knownPayoutTxs$TxHash,]
  payoutList <- payoutList[!paste0(payoutList$TxHash, payoutList$Miner) %in% paste0(knownPayouts$TxHash, knownPayouts$Miner),]
  
  if(nrow(minerList)>0){
    mysql_fast_db_write_table(con, "miner",minerList, append = TRUE)
  }
  
  if(nrow(payoutTransactions)>0){
    mysql_fast_db_write_table(con, "payouttransaction",payoutTransactions, append = TRUE)
  }
  
  if(nrow(payoutList)>0){
    mysql_fast_db_write_table(con, "payout",payoutList, append = TRUE)
  }
  
}

dbDisconnect(con)
