-- changeset VArkhipov:create_upload_external_table
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.tables where table_name = 'upload_external_table' and table_schema = database();
create table upload_external_table (
  id bigint not null auto_increment primary key,
  datamart_id bigint not null,
  table_name varchar(100) not null,
  type_id integer not null,
  location_path varchar(1024) not null,
  format_id integer not null,
  table_schema text not null,
  message_limit integer,
  constraint upload_external_table_schema foreign key(datamart_id) references datamarts_registry(datamart_id) on delete cascade,
  constraint upload_external_table_type foreign key(type_id) references download_external_type(id),
  constraint upload_external_table_format foreign key(format_id) references download_external_format(id),
  constraint upload_external_table_ak1 unique(datamart_id, table_name)
);
-- rollback drop table upload_external_table;

-- changeset VArkhipov:create_upload_query
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.tables where table_name = 'upload_query' and table_schema = database();
create table upload_query (
  id char(36) not null primary key,
  datamart_id bigint not null,
  table_name_ext varchar(100) not null,
  table_name_dst varchar(100) not null,
  sql_query text not null,
  status integer not null,
  constraint upload_query_datamart foreign key(datamart_id) references upload_external_table(datamart_id)
);
-- rollback drop table upload_query;