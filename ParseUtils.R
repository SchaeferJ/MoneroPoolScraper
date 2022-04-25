process_blockstring <- function(blockstring){
  minedBlockHeights <- blockstring[seq(2,length(blockstring),2)]
  minedBlocks <- blockstring[seq(1,length(blockstring),2)]
  
  minedBlocks <- strsplit(minedBlocks,":")
  minedBlocks <- lapply(minedBlocks, function(x) data.frame(t(x)))
  minedBlocks <- bind_rows(minedBlocks)
  names(minedBlocks) <- c("Hash", "Date", "Difficulty","UNK","Maturity","Reward")
  minedBlocks$Height <- minedBlockHeights
  return(minedBlocks)
}

process_paymentstring <- function(paymentstring){
  paymentDates <- paymentstring[seq(2,length(paymentstring),2)]
  payments <- paymentstring[seq(1,length(paymentstring),2)]
  
  payments <- strsplit(payments, ":")
  payments <- lapply(payments, function(x) data.frame(t(x)))
  payments <- bind_rows(payments)
  names(payments) <- c("TxHash", "Amount", "Fee", "Mixin", "Payees")
  payments$Date <- paymentDates
  return(payments)
}