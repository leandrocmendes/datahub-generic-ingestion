CREATE DATABASE BI;

USE BI;

CREATE TABLE SALES (
	id int not null auto_increment primary key,
	sale_date datetime,
	quantity float,
	total float
);