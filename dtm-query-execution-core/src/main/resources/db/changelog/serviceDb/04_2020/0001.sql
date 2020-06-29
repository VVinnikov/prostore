-- liquibase formatted sql

-- changeset AISamoylov:create_datamarts_registry_table
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.tables where table_name = 'datamarts_registry' and table_schema = database();
create table datamarts_registry  (
  datamart_id bigint not null auto_increment primary key,
  datamart_mnemonics varchar(1024) not null
);
-- rollback drop table datamarts_registry;

-- changeset AISamoylov:create_entities_registry_table
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.tables where table_name = 'entities_registry' and table_schema = database();
create table entities_registry (
  entity_id bigint not null auto_increment primary key,
  datamart_id bigint not null references datamarts_registry(datamart_id),
  entity_mnemonics varchar(1024)  not null
);
-- rollback drop table entities_registry;

-- changeset AISamoylov:create_data_types_registry_table
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.tables where table_name = 'data_types_registry' and table_schema = database();
create table data_types_registry (
  data_type_id int not null auto_increment primary key,
  data_type_mnemonics varchar(1024)  not null
);
-- rollback drop table data_types_registry;

-- changeset AISamoylov:create_attributes_registry_table
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.tables where table_name = 'attributes_registry' and table_schema = database();
create table attributes_registry (
  attr_id bigint not null auto_increment primary key auto_increment,
  entity_id bigint not null references entities_registry(entity_id),
  data_type_id int not null references data_types_registry(data_type_id),
  attr_mnemonics varchar(1024)  not null,
  length int,
  accuracy int
);
-- rollback drop table attributes_registry;

-- changeset AISamoylov:create_download_external_type
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.tables where table_name = 'download_external_type' and table_schema = database();
create table download_external_type(
  id integer not null primary key,
  name varchar(100) not null,
  constraint download_external_type_ak1 unique(name)
);
-- rollback drop table download_external_type;

-- changeset AISamoylov:create_download_external_format
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.tables where table_name = 'download_external_format' and table_schema = database();
create table download_external_format(
  id integer not null primary key,
  name varchar(100) not null,
  constraint download_external_format_ak1 unique(name)
);
-- rollback drop table download_external_format;

-- changeset AISamoylov:create_download_external_table
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.tables where table_name = 'download_external_table' and table_schema = database();
create table download_external_table(
  id bigint not null auto_increment primary key,
  schema_id bigint not null,
  table_name varchar(100) not null,
  type_id integer not null,
  location varchar(100) not null,
  format_id integer not null,
  chunk_size int null,
  constraint download_external_table_schema foreign key(schema_id) references datamarts_registry(datamart_id) on delete cascade,
  constraint download_external_table_type foreign key(type_id) references download_external_type(id),
  constraint download_external_table_format foreign key(format_id) references download_external_format(id),
  constraint download_external_table_ak1 unique(schema_id, table_name)
);
-- rollback drop table download_external_table;

-- changeset AISamoylov:create_download_query
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.tables where table_name = 'download_query' and table_schema = database();
create table download_query(
  id char(36) not null primary key,
  det_id bigint not null,
  sql_query text not null,
  constraint download_query_det foreign key(det_id) references download_external_table(id) on delete cascade
);
-- rollback drop table download_query;

-- changeset AISamoylov:create_delta_data
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.tables where table_name = 'delta_data' and table_schema = database();
create table delta_data (
  load_id bigint not null primary key auto_increment,
  datamart_mnemonics varchar(1024) not null,
  sys_date datetime,
  sin_id bigint,
  load_proc_id  varchar(1024),
  status integer not null
);
-- rollback drop table delta_data;
