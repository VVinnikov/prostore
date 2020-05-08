--liquibase formatted sql

--changeset OTelezhnikova:create_fk_entities_registry_to_datamart_id
--preconditions onFail:MARK_RAN
--precondition-sql-check expectedResult:0 select count(*) from information_schema.table_constraints where constraint_name = 'fk_entity_to_datamart' and table_schema = database();
ALTER TABLE entities_registry ADD CONSTRAINT fk_entity_to_datamart
FOREIGN KEY (datamart_id) REFERENCES datamarts_registry (datamart_id)
ON DELETE CASCADE;
--rollback ALTER TABLE entities_registry DROP FOREIGN KEY fk_entity_to_datamart;

--changeset OTelezhnikova:create_fk_attributes_registry_to_entity_id
--preconditions onFail:MARK_RAN
--precondition-sql-check expectedResult:0 select count(*) from information_schema.table_constraints where constraint_name = 'fk_attr_to_entity' and table_schema = database();
ALTER TABLE attributes_registry ADD CONSTRAINT fk_attr_to_entity
FOREIGN KEY (entity_id) REFERENCES entities_registry (entity_id)
ON DELETE CASCADE;
--rollback ALTER TABLE attributes_registry DROP FOREIGN KEY fk_attr_to_entity;
