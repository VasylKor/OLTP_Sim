CREATE TABLE `names` (
  `Id` bigint not null,
  `name` varchar(255) DEFAULT NULL,
  `surname` varchar(255) DEFAULT NULL,
  `sex` varchar(50) DEFAULT NULL,
  `country_code` varchar(100) DEFAULT null,
  primary key (Id)
) ENGINE=InnoDB 


CREATE TABLE `addresses` (
  `Id` bigint not null,
  `LON` double DEFAULT NULL,
  `LAT` double DEFAULT NULL,
  `NUMBER` varchar(100) DEFAULT NULL,
  `STREET` varchar(255) DEFAULT NULL,
  `UNIT` varchar(100) DEFAULT NULL,
  `CITY` varchar(200) DEFAULT NULL,
  `DISTRICT` varchar(100) DEFAULT NULL,
  `REGION` varchar(100) DEFAULT NULL,
  `POSTCODE` varchar(100) DEFAULT null,
  primary key (Id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE alphashop.customers (
	Id bigint(20) auto_increment NOT NULL,
	name varchar(255) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL NULL,
	surname varchar(255) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL NULL,
	sex varchar(50) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL NULL,
	nationality varchar(100) CHARACTER SET latin1 COLLATE latin1_swedish_ci DEFAULT NULL NULL,
	address_id BIGINT NULL,
	CONSTRAINT `PRIMARY` PRIMARY KEY (Id),
	CONSTRAINT names_FK FOREIGN KEY (address_id) REFERENCES alphashop.addresses(Id) ON DELETE SET NULL
)
ENGINE=InnoDB
DEFAULT CHARSET=latin1
COLLATE=latin1_swedish_ci
COMMENT='';



create table alphashop.receipts(
	receipt_id UUID not null primary key,
	shop_id smallint,
	total float,
	customer_id bigint, 
	`date` date,
	`time` time,
	constraint fk_customer_id foreign key (customer_id) references alphashop.customers (Id)
)


