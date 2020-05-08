--liquibase formatted sql

--changeset KRozenkov:create_logic_schema_datamarts
--preconditions onFail:MARK_RAN
--precondition-sql-check expectedResult:0 select count(*) from information_schema.tables where table_name = 'logic_schema_datamarts' and table_schema = database();
create view logic_schema_datamarts as
select null as catalog_name,
       datamart_mnemonics as schema_name
from datamarts_registry;
--rollback drop view logic_schema_datamarts;

--changeset KRozenkov:create_logic_schema_entities
--preconditions onFail:MARK_RAN
--precondition-sql-check expectedResult:0 select count(*) from information_schema.tables where table_name = 'logic_schema_entities' and table_schema = database();
create view logic_schema_entities as
select null as table_catalog,
       datamart_mnemonics as table_schema,
       entity_mnemonics as table_name,
       'BASE TABLE' as table_type
from entities_registry entities
inner join datamarts_registry datamarts on datamarts.datamart_id = entities.datamart_id;
--rollback drop view logic_schema_entities;

--changeset KRozenkov:add_status_date_to_delta_data
--preconditions onFail:MARK_RAN
--precondition-sql-check expectedResult:0 select count(*) from information_schema.columns where column_name = 'status_dat' and table_name = 'delta_data' and table_schema = database();
alter table delta_data add status_date datetime;
--rollback alter table delta_data drop status_date;

--changeset KRozenkov:create_logic_schema_deltas
--preconditions onFail:MARK_RAN
--precondition-sql-check expectedResult:0 select count(*) from information_schema.tables where table_name = 'logic_schema_deltas' and table_schema = database()
create view logic_schema_deltas as
select null as delta_catalog,
       datamart_mnemonics as delta_schema,
       load_id,
       sys_date,
       sin_id,
       status,
       status_date
from delta_data;
--rollback drop view logic_schema_deltas;
