-- changeset ilapa:create_download_external_table_attributes_table
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.tables where table_name = 'download_external_table_attributes' and table_schema = database();
create table if not exists download_external_table_attribute
(
    column_name  varchar(100) not null,
    det_id       bigint       not null references download_external_table (id),
    data_type_id int          not null references data_types_registry (data_type_id),
    data_type     varchar(32)  not null,
    order_num    int,
    PRIMARY KEY (column_name, det_id)
);
-- rollback drop table download_external_table_attributes;
