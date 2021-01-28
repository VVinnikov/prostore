
use demomvp;

select * from accounts;
select * from transactions;


select * from information_schema.DELTAS where delta_schema = 'demomvp';

select count(*) from accounts DATASOURCE_TYPE = 'ADB';
select count(*) from accounts DATASOURCE_TYPE = 'ADG';
select count(*) from accounts DATASOURCE_TYPE = 'ADQM';
select count(*) from transactions DATASOURCE_TYPE = 'ADB';
select count(*) from transactions DATASOURCE_TYPE = 'ADG';
select count(*) from transactions DATASOURCE_TYPE = 'ADQM';

-- commit delta;

select count(*) from transactions for system_time as of latest_uncommitted_delta DATASOURCE_TYPE = 'ADB';

select count(*) from transactions;

select count(*) from transactions FOR SYSTEM_TIME as of delta_num 1;

select count(*) from transactions FOR SYSTEM_TIME as of delta_num 1 DATASOURCE_TYPE = 'ADQM';

select count(*) from transactions FOR SYSTEM_TIME as of '' DATASOURCE_TYPE = 'ADG';
