-- changeset ilapa:create_views_registry
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.tables where table_name = 'views_registry' and table_schema = database();
create table if not exists views_registry
(
    view_name       varchar(100)  not null ,
    datamart_id bigint        not null references datamarts_registry (datamart_id),
    query     varchar(2048)  not null,
    PRIMARY KEY (view_name, datamart_id)
);
-- rollback drop table views_registry;
