setwd("/home/jochen/Poolscraper")
library(DBI)
library(htmltab)
source("./DButils.R")

con <- dbConnect(RMariaDB::MariaDB(), user=DB_USERNAME, password=DB_PW, dbname="monero")
# General Setup
POOL_NAME <- "Miningpoolhub"
POOL_URL <- "https://monero.miningpoolhub.com/"

# Preparation: Check if Pool is known to Database (required as Miner references the Pool Name as Foreign Key)
check_pool(POOL_NAME, POOL_URL)

# Define API-Endpoints to query
BLOCKS_ENDPOINT <- "https://monero.miningpoolhub.com/index.php?page=statistics&action=blocks"
PAYMENT_ENDPOINT <- "https://np-api.monerod.org/pool/payments?page=%i&limit=%i"

# Rate Limit (Max API Hits per Minute)
RATE_LIMIT <- 30

# Preparation: Check if Pool is known to Database (required as Miner references the Pool Name as Foreign Key)
check_pool(POOL_NAME, POOL_URL)

# Preparation: Check for newest known block
lastKnownBlock <- get_latest_poolblock(POOL_NAME)

minedBlocks <- htmltab(BLOCKS_ENDPOINT, which = 4)
minedBlocks <- minedBlocks[minedBlocks$Validity=="Confirmed",]
minedBlocks <- minedBlocks[,c("Block","Time","Difficulty", "Amount")]
minedBlocks$Block <- as.numeric(minedBlocks$Block)
minedBlocks$Amount <- as.numeric(minedBlocks$Amount)
minedBlocks$Difficulty <- as.numeric(gsub(",","", minedBlocks$Difficulty))
minedBlocks$Time <- as.POSIXct(minedBlocks$Time, format="%d/%m %H:%M:%S (UTC)", tz="GMT")

names(minedBlocks) <- c("Height", "Date", "Difficulty", "Reward")
minedBlocks$Pool <- POOL_NAME
minedBlocks$Timestamp <- Sys.time()

minedBlocks <- minedBlocks[minedBlocks$Height > lastKnownBlock,]

mysql_fast_db_write_table(con, "block",minedBlocks, append = TRUE)
dbDisconnect(con)
