process_blockstring <- function(blockstring, skip=0){
  minedBlockHeights <- blockstring[seq(2,length(blockstring),2)]
  minedBlocks <- blockstring[seq(1,length(blockstring),2)]
  
  minedBlocks <- strsplit(minedBlocks,":")
  minedBlocks <- lapply(minedBlocks, function(x) data.frame(t(x), stringsAsFactors = FALSE))
  minedBlocks <- bind_rows(minedBlocks)
  if(skip>0){
    minedBlocks <- minedBlocks[,-c(1:skip)]
  }
  names(minedBlocks) <- c("Hash", "Date", "Difficulty","UNK","Maturity","Reward")
  minedBlocks$Height <- minedBlockHeights
  return(minedBlocks)
}

process_paymentstring <- function(paymentstring, skip=0){
  paymentDates <- paymentstring[seq(2,length(paymentstring),2)]
  payments <- paymentstring[seq(1,length(paymentstring),2)]
  
  payments <- strsplit(payments, ":")
  payments <- lapply(payments, function(x) data.frame(t(x), stringsAsFactors = FALSE))
  payments <- bind_rows(payments)
  if(skip>0){
    payments <- payments[,-c(1:skip)]
  }
  names(payments) <- c("TxHash", "Amount", "Fee", "Mixin", "Payees")
  payments$Date <- paymentDates
  return(payments)
}

process_paymentstring_xmrpool <- function(paymentstring, skip=0){
  paymentDates <- paymentstring[seq(2,length(paymentstring),2)]
  payments <- paymentstring[seq(1,length(paymentstring),2)]
  
  payments <- strsplit(payments, ":")
  payments <- lapply(payments, function(x) data.frame(t(x), stringsAsFactors = FALSE))
  payments <- bind_rows(payments)
  if(skip>0){
    payments <- payments[,-c(1:skip)]
  }
  names(payments) <- c("TxHash", "Amount", "UNK", "Mixin")
  payments$Date <- paymentDates
  return(payments)
}


process_paymentstring_minexmr <- function(paymentstring, skip=0){
  payments <- strsplit(paymentstring, ":")
  payments <- lapply(payments, function(x) data.frame(t(x), stringsAsFactors = FALSE))
  payments <- bind_rows(payments)
  names(payments) <- c("Date", "TxHash", "Amount", "Fee", "UNK")
  return(payments)
}
