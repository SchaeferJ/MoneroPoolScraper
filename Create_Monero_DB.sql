CREATE TABLE `Miner` (
	`Address` varchar(110) NOT NULL,
	`Pool` varchar(255) NOT NULL,
	`Balance` FLOAT NOT NULL,
	`UncoBalance` FLOAT NOT NULL,
	`TotalPayout` FLOAT,
	`BlocksFound` INTEGER,
	`Hashrate` FLOAT NOT NULL,
	`H1` FLOAT,
	`H3` FLOAT,
	`H6` FLOAT,
	`H12` FLOAT,
	`H24` FLOAT,
	`Timestamp` TIMESTAMP NOT NULL,
	PRIMARY KEY (`Address`,`Pool`)
);

CREATE TABLE `Pool` (
	`Name` varchar(255) NOT NULL,
	`URL` varchar(255) NOT NULL,
	`Timestamp` TIMESTAMP NOT NULL,
	PRIMARY KEY (`Name`)
);

CREATE TABLE `Block` (
	`Height` INT NOT NULL,
	`Miner` varchar(110),
	`Hash` varchar(110) NOT NULL,
	`Pool` varchar(255) NOT NULL,
	`Date` DATETIME,
	`Reward` FLOAT,
	`Difficulty`BIGINT UNSIGNED, 
	`MinerHash` varchar(50) NULL,
	`Timestamp` TIMESTAMP NOT NULL,
	PRIMARY KEY (`Height`)
);

CREATE TABLE `Payout` (
	`TxHash` varchar(255) NOT NULL,
	`Miner` varchar(110) NOT NULL,
	`Amount` FLOAT NOT NULL,
	`Timestamp` TIMESTAMP NOT NULL,
	PRIMARY KEY (`TxHash`,`Miner`)
);

CREATE TABLE `PayoutTransaction` (
	`TxHash` varchar(255) NOT NULL,
	`Date` DATETIME,
	`Pool` VARCHAR(255) NOT NULL,
	`Fee` FLOAT,
	`Amount` FLOAT,
	`Payees` INT,
	`Mixin`TINYINT,
	`Timestamp` TIMESTAMP NOT NULL,
	PRIMARY KEY (`TxHash`)
);

CREATE TABLE `Worker` (
	`WorkerID` varchar(100) NOT NULL,
	`Miner` varchar(110) NOT NULL,
	`Pool` varchar(255) NOT NULL,
	`UID` BIGINT UNSIGNED NOT NULL,
	`Hashrate` FLOAT NOT NULL,
	`LastShare` DATETIME NOT NULL,
	`Rating` INT NOT NULL,
	`H1` FLOAT NOT NULL,
	`H3` FLOAT NOT NULL,
	`H6` FLOAT NOT NULL,
	`H12` FLOAT NOT NULL,
	`H24` FLOAT NOT NULL,
	`Timestamp` TIMESTAMP NOT NULL,
	PRIMARY KEY (`UID`)
);

CREATE TABLE `Hashrate` (
	`Miner` varchar(110) NOT NULL,
	`RefTime` DATETIME NOT NULL,
	`Hashrate` INT NOT NULL,
	`WorkersOnline` SMALLINT UNSIGNED,
	`Timestamp` TIMESTAMP NOT NULL,
	PRIMARY KEY (`Miner`,`RefTime`)
);

CREATE TABLE `Sharerate` (
	`Miner` varchar(110) NOT NULL,
	`RefTime` DATETIME NOT NULL,
	`Shares` INT NOT NULL,
	`Timestamp` TIMESTAMP NOT NULL,
	PRIMARY KEY (`Miner`,`RefTime`)
);

ALTER TABLE `Miner` ADD CONSTRAINT `Miner_fk0` FOREIGN KEY (`Pool`) REFERENCES `Pool`(`Name`);

ALTER TABLE `Block` ADD CONSTRAINT `Block_fk0` FOREIGN KEY (`Miner`) REFERENCES `Miner`(`Address`);

ALTER TABLE `Block` ADD CONSTRAINT `Block_fk1` FOREIGN KEY (`Pool`) REFERENCES `Pool`(`Name`);

ALTER TABLE `Payout` ADD CONSTRAINT `Payout_fk0` FOREIGN KEY (`TxHash`) REFERENCES `PayoutTransaction`(`TxHash`);

ALTER TABLE `Payout` ADD CONSTRAINT `Payout_fk1` FOREIGN KEY (`Miner`) REFERENCES `Miner`(`Address`);

ALTER TABLE `PayoutTransaction` ADD CONSTRAINT `PayoutTransaction_fk0` FOREIGN KEY (`Pool`) REFERENCES `Pool`(`Name`);

ALTER TABLE `Worker` ADD CONSTRAINT `Worker_fk0` FOREIGN KEY (`Miner`) REFERENCES `Miner`(`Address`);

ALTER TABLE `Worker` ADD CONSTRAINT `Worker_fk1` FOREIGN KEY (`Pool`) REFERENCES `Pool`(`Name`);

ALTER TABLE `Hashrate` ADD CONSTRAINT `Hashrate_fk0` FOREIGN KEY (`Miner`) REFERENCES `Miner`(`Address`);

ALTER TABLE `Sharerate` ADD CONSTRAINT `Sharerate_fk0` FOREIGN KEY (`Miner`) REFERENCES `Miner`(`Address`);








