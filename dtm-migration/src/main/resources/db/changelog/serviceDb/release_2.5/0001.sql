-- changeset VArkhipov:drop_upload_query_datamart
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:1 select count(*) from information_schema.table_constraints where constraint_name = 'upload_query_datamart' and table_schema = database();
alter table upload_query drop foreign key upload_query_datamart;
-- rollback alter table upload_query drop foreign key upload_query_datamart;

-- changeset VArkhipov:create_upload_query_datamart
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.table_constraints where constraint_name = 'upload_query_datamart' and table_schema = database();
alter table upload_query add constraint upload_query_datamart foreign key (datamart_id)
references upload_external_table (datamart_id) on delete cascade;
-- rollback alter table upload_query add constraint upload_query_datamart foreign key (datamart_id) references upload_external_table (datamart_id) on delete cascade;