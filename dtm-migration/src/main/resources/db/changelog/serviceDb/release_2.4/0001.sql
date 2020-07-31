-- changeset akapustin:add_avro_schema_to_download_external_table
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.COLUMNS where table_name = 'download_external_table' and table_schema = database() and column_name = 'table_schema';
alter table download_external_table
    add table_schema text default '' not null;
