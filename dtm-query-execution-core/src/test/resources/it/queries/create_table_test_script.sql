create database it_test_1;
use it_test_1;
create table transactions (
     transaction_id bigint not null,
     transaction_date varchar(20),
     account_id bigint not null,
     amount bigint,
     primary key (transaction_id))
distributed by (transaction_id);
drop table transactions;
drop database it_test_1;