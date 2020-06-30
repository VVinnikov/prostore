-- changeset VArkhipov:alter_download_query
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:1 select count(*) from information_schema.COLUMNS where table_name = 'download_query' and table_schema = database() and column_name = 'det_id';
alter table download_query change det_id datamart_id bigint not null;
-- rollback alter table download_query change det_id datamart_id bigint not null;

-- changeset VArkhipov:add_table_name_ext_to_download_query
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.COLUMNS where table_name = 'download_query' and table_schema = database() and column_name = 'table_name_ext';
alter table download_query add table_name_ext varchar(100) not null;
-- rollback alter table download_query add table_name_ext varchar(100) not null;

-- changeset VArkhipov:add_status_to_download_query
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.COLUMNS where table_name = 'download_query' and table_schema = database() and column_name = 'status';
alter table download_query add status integer not null;
-- rollback alter table download_query add status integer not null;

-- changeset VArkhipov:delete_constraint_download_query
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:1 select count(*) from information_schema.TABLE_CONSTRAINTS where TABLE_NAME = 'download_query' and table_schema = database() and CONSTRAINT_NAME = 'download_query_det';
alter table download_query drop foreign key download_query_det;
-- rollback alter table download_query drop foreign key download_query_det;

-- changeset VArkhipov:add_datamart_constraint_download_query
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.TABLE_CONSTRAINTS where TABLE_NAME = 'download_query' and table_schema = database() and CONSTRAINT_NAME = 'download_query_datamart';
alter table download_query add constraint download_query_datamart foreign key (datamart_id) references datamarts_registry (datamart_id) on delete cascade;
-- rollback alter table download_query add constraint download_query_datamart foreign key (datamart_id) references datamarts_registry (datamart_id) on delete cascade;