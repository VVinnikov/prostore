-- liquibase formatted sql

-- changeset dkosiakov:create_logic_schema_attributes
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.tables where table_name = 'logic_schema_attributes' and table_schema = database();
CREATE VIEW logic_schema_attributes AS
SELECT null AS table_catalog, datamart_mnemonics AS table_schema, entity_mnemonics AS table_name, attr.attr_mnemonics AS column_name, attr.nullable AS is_nullable, dt.data_type_mnemonics AS data_type, attr.length AS character_maximum_length, attr.accuracy AS datetime_precision
FROM attributes_registry attr
         INNER JOIN data_types_registry dt ON dt.data_type_id = attr.data_type_id
         INNER JOIN entities_registry entity ON entity.entity_id = attr.entity_id
         INNER JOIN datamarts_registry datamarts ON datamarts.datamart_id = entity.datamart_id;
-- rollback drop view logic_schema_attributes;

-- changeset dkosiakov:create_logic_schema_table_constraints
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.tables where table_name = 'logic_schema_table_constraints' and table_schema = database();
CREATE VIEW logic_schema_table_constraints AS
SELECT null AS constraint_catalog, datamart_mnemonics AS constraint_schema, CONCAT(datamart_mnemonics,'_','entity_mnemonics','_','pk') AS constraint_name, datamart_mnemonics AS table_schema, entity_mnemonics AS table_name, 'primary key' AS CONSTRAINT_TYPE
FROM entities_registry entity
         INNER JOIN datamarts_registry datamarts ON datamarts.datamart_id = entity.datamart_id
         INNER JOIN attributes_registry attr ON attr.entity_id = entity.entity_id
WHERE attr.primary_key_order IS NOT NULL
GROUP BY entity.datamart_id, entity.entity_id
UNION ALL
SELECT null AS constraint_catalog, datamart_mnemonics AS constraint_schema, CONCAT(datamart_mnemonics,'_','entity_mnemonics','_','sk') AS constraint_name, datamart_mnemonics AS table_schema, entity_mnemonics AS table_name, 'sharding key' AS CONSTRAINT_TYPE
FROM entities_registry entity
         INNER JOIN datamarts_registry datamarts ON datamarts.datamart_id = entity.datamart_id
         INNER JOIN attributes_registry attr ON attr.entity_id = entity.entity_id
WHERE attr.distribute_key_order IS NOT NULL
GROUP BY entity.datamart_id, entity.entity_id;
-- rollback drop view logic_schema_table_constraints;

-- changeset dkosiakov:create_logic_schema_key_column_usage
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.tables where table_name = 'logic_schema_key_column_usage' and table_schema = database();
CREATE VIEW logic_schema_key_column_usage AS
SELECT null AS constraint_catalog, datamart_mnemonics AS constraint_schema,
       CONCAT(datamart_mnemonics,'_','entity_mnemonics','_','pk') AS constraint_name, datamart_mnemonics AS table_schema, entity_mnemonics AS table_name, attr.attr_mnemonics AS column_name, attr.primary_key_order AS ordinal_position
FROM entities_registry entity
         INNER JOIN datamarts_registry datamarts ON datamarts.datamart_id = entity.datamart_id
         INNER JOIN attributes_registry attr ON attr.entity_id = entity.entity_id
WHERE attr.primary_key_order IS NOT NULL
UNION ALL
SELECT null AS constraint_catalog, datamart_mnemonics AS constraint_schema,
       CONCAT(datamart_mnemonics,'_','entity_mnemonics','_','sk') AS constraint_name, datamart_mnemonics AS table_schema, entity_mnemonics AS table_name, attr.attr_mnemonics AS column_name, attr.distribute_key_order AS ordinal_position
FROM entities_registry entity
         INNER JOIN datamarts_registry datamarts ON datamarts.datamart_id = entity.datamart_id
         INNER JOIN attributes_registry attr ON attr.entity_id = entity.entity_id
WHERE attr.distribute_key_order IS NOT NULL;
-- rollback drop view logic_schema_key_column_usage;
