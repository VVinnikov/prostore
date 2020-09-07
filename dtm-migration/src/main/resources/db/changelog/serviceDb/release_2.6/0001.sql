-- changeset VArkhipov:drop_attributes_registry_ibfk_1
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:1 select count(*) from information_schema.table_constraints where constraint_name = 'attributes_registry_ibfk_1' and table_schema = database();
alter table attributes_registry drop foreign key attributes_registry_ibfk_1;
-- rollback alter table attributes_registry drop foreign key attributes_registry_ibfk_1;

-- changeset VArkhipov:drop_download_external_table_attribute_ibfk_1
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:1 select count(*) from information_schema.table_constraints where constraint_name = 'download_external_table_attribute_ibfk_1' and table_schema = database();
alter table download_external_table_attribute drop foreign key download_external_table_attribute_ibfk_1;
-- rollback alter table download_external_table_attribute drop foreign key download_external_table_attribute_ibfk_1;

-- changeset VArkhipov:drop_entities_registry_ibfk_1
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:1 select count(*) from information_schema.table_constraints where constraint_name = 'entities_registry_ibfk_1' and table_schema = database();
alter table entities_registry drop foreign key entities_registry_ibfk_1;
-- rollback alter table entities_registry drop foreign key entities_registry_ibfk_1;

-- changeset VArkhipov:drop_views_registry_ibfk_1
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:1 select count(*) from information_schema.table_constraints where constraint_name = 'views_registry_ibfk_1' and table_schema = database();
alter table views_registry drop foreign key views_registry_ibfk_1;
-- rollback alter table views_registry drop foreign key views_registry_ibfk_1;

-- changeset VArkhipov:сreate_fk_attribute_to_download_external_table
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.table_constraints where constraint_name = 'fk_attribute_to_download_external_table' and table_schema = database();
alter table download_external_table_attribute add constraint fk_attribute_to_download_external_table foreign key (det_id) references download_external_table (id) on delete cascade;
-- rollback alter table download_external_table_attribute add constraint fk_attribute_to_download_external_table foreign key (det_id) references download_external_table (id) on delete cascade;

-- changeset VArkhipov:сreate_fk_views_to_datamart
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.table_constraints where constraint_name = 'fk_views_to_datamart' and table_schema = database();
alter table views_registry add constraint fk_views_to_datamart foreign key (datamart_id) references datamarts_registry (datamart_id) on delete cascade;
-- rollback alter table views_registry add constraint fk_views_to_datamart foreign key (datamart_id) references datamarts_registry (datamart_id);
