CREATE UPLOAD EXTERNAL TABLE %s.%s
(
    transaction_id bigint not null,
    transaction_date varchar(20),
    account_id bigint not null,
    amount bigint
)
LOCATION 'kafka://%s/%s'
FORMAT 'AVRO';