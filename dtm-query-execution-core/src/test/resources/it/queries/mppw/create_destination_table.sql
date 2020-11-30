create table %s.%s
(
    transaction_id bigint not null,
    transaction_date varchar(20),
    account_id bigint not null,
    amount bigint,
    primary key (transaction_id)
) distributed by (transaction_id)
datasource_type(%s);