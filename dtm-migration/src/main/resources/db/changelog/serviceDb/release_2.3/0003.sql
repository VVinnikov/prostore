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

-- changeset dkosiakov::add_attributes_to_system_views_registry
INSERT INTO system_views_registry (name, query, `schema`)
VALUES ('ATTRIBUTES',
        'SELECT null AS table_catalog, datamart_mnemonics AS table_schema, entity_mnemonics AS table_name, attr.attr_mnemonics AS column_name, attr.nullable AS is_nullable, dt.data_type_mnemonics AS data_type, attr.length AS character_maximum_length, attr.accuracy AS datetime_precision
          FROM attributes_registry attr
          INNER JOIN data_types_registry dt ON dt.data_type_id = attr.data_type_id
          INNER JOIN entities_registry entity ON entity.entity_id = attr.entity_id
          INNER JOIN datamarts_registry datamarts ON datamarts.datamart_id = entity.datamart_id',
'{"id":"1ce89a72-d194-11ea-87d0-000000000001","mnemonic":"ATTRIBUTES","datamartMnemonic":"information_schema","label":"ATTRIBUTES",
"tableAttributes":[
{"id":"1ce89a72-d194-11ea-87d0-000000000002","mnemonic":"table_catalog","type":
{"id":"1ce89a72-d194-11ea-87d0-000000000003","value":"VARCHAR"},
"length":null,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":0,"nullable":true},
{"id":"1ce89a72-d194-11ea-87d0-000000000004","mnemonic":"table_schema","type":
{"id":"1ce89a72-d194-11ea-87d0-000000000005","value":"VARCHAR"},
"length":null,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":1,"nullable":true},
{"id":"1ce89a72-d194-11ea-87d0-000000000006","mnemonic":"table_name","type":
{"id":"1ce89a72-d194-11ea-87d0-000000000007","value":"VARCHAR"},
"length":null,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":2,"nullable":true},
{"id":"1ce89a72-d194-11ea-87d0-000000000008","mnemonic":"column_name","type":
{"id":"1ce89a72-d194-11ea-87d0-000000000009","value":"VARCHAR"},
"length":null,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":3,"nullable":true},
{"id":"1ce89a72-d194-11ea-87d0-000000000010","mnemonic":"is_nullable","type":
{"id":"1ce89a72-d194-11ea-87d0-000000000011","value":"INT"},
"length":null,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":4,"nullable":true},
{"id":"1ce89a72-d194-11ea-87d0-000000000012","mnemonic":"data_type","type":
{"id":"1ce89a72-d194-11ea-87d0-000000000013","value":"VARCHAR"},
"length":null,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":5,"nullable":true},
{"id":"1ce89a72-d194-11ea-87d0-000000000014","mnemonic":"character_maximum_length","type":
{"id":"1ce89a72-d194-11ea-87d0-000000000015","value":"INT"},
"length":null,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":6,"nullable":true},
{"id":"1ce89a72-d194-11ea-87d0-000000000016","mnemonic":"datetime_precision","type":
{"id":"1ce89a72-d194-11ea-87d0-000000000017","value":"INT"},
"length":null,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":7,"nullable":true}
],"primaryKeys":null}');
-- rollback DELETE FROM system_views_registry where name='ATTRIBUTES';

-- changeset dkosiakov:create_logic_schema_table_constraints
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.tables where table_name = 'logic_schema_table_constraints' and table_schema = database();
CREATE VIEW logic_schema_table_constraints AS
SELECT null AS constraint_catalog, datamart_mnemonics AS constraint_schema, CONCAT(datamart_mnemonics,'_','entity_mnemonics','_','pk') AS constraint_name, datamart_mnemonics AS table_schema, entity_mnemonics AS table_name, 'primary key' AS constraint_type
FROM entities_registry entity
         INNER JOIN datamarts_registry datamarts ON datamarts.datamart_id = entity.datamart_id
         INNER JOIN attributes_registry attr ON attr.entity_id = entity.entity_id
WHERE attr.primary_key_order IS NOT NULL
GROUP BY entity.datamart_id, entity.entity_id
UNION ALL
SELECT null AS constraint_catalog, datamart_mnemonics AS constraint_schema, CONCAT(datamart_mnemonics,'_','entity_mnemonics','_','sk') AS constraint_name, datamart_mnemonics AS table_schema, entity_mnemonics AS table_name, 'sharding key' AS constraint_type
FROM entities_registry entity
         INNER JOIN datamarts_registry datamarts ON datamarts.datamart_id = entity.datamart_id
         INNER JOIN attributes_registry attr ON attr.entity_id = entity.entity_id
WHERE attr.distribute_key_order IS NOT NULL
GROUP BY entity.datamart_id, entity.entity_id;
-- rollback drop view logic_schema_table_constraints;

-- changeset dkosiakov::add_table_constraints_to_system_views_registry
INSERT INTO system_views_registry (name, query, `schema`)
VALUES ('TABLE_CONSTRAINTS',
        'SELECT null AS constraint_catalog, datamart_mnemonics AS constraint_schema, CONCAT(datamart_mnemonics,''_'',''entity_mnemonics'',''_'',''pk'') AS constraint_name, datamart_mnemonics AS table_schema, entity_mnemonics AS table_name, ''primary key'' AS constraint_type
         FROM entities_registry entity
         INNER JOIN datamarts_registry datamarts ON datamarts.datamart_id = entity.datamart_id
         INNER JOIN attributes_registry attr ON attr.entity_id = entity.entity_id
         WHERE attr.primary_key_order IS NOT NULL
         GROUP BY entity.datamart_id, entity.entity_id
         UNION ALL
         SELECT null AS constraint_catalog, datamart_mnemonics AS constraint_schema, CONCAT(datamart_mnemonics,''_'',''entity_mnemonics'',''_'',''sk'') AS constraint_name, datamart_mnemonics AS table_schema, entity_mnemonics AS table_name, ''sharding key'' AS constraint_type
         FROM entities_registry entity
         INNER JOIN datamarts_registry datamarts ON datamarts.datamart_id = entity.datamart_id
         INNER JOIN attributes_registry attr ON attr.entity_id = entity.entity_id
         WHERE attr.distribute_key_order IS NOT NULL
         GROUP BY entity.datamart_id, entity.entity_id',
'{"id":"63335ce0-d1a0-11ea-87d0-000000000001","mnemonic":"TABLE_CONSTRAINTS","datamartMnemonic":"information_schema","label":"TABLE_CONSTRAINTS",
"tableAttributes":[
{"id":"63335ce0-d1a0-11ea-87d0-000000000002","mnemonic":"constraint_catalog","type":
{"id":"63335ce0-d1a0-11ea-87d0-000000000003","value":"VARCHAR"},
"length":null,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":0,"nullable":true},
{"id":"63335ce0-d1a0-11ea-87d0-000000000004","mnemonic":"constraint_schema","type":
{"id":"63335ce0-d1a0-11ea-87d0-000000000005","value":"VARCHAR"},
"length":null,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":1,"nullable":true},
{"id":"63335ce0-d1a0-11ea-87d0-000000000006","mnemonic":"constraint_name","type":
{"id":"63335ce0-d1a0-11ea-87d0-000000000007","value":"VARCHAR"},
"length":null,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":2,"nullable":true},
{"id":"63335ce0-d1a0-11ea-87d0-000000000008","mnemonic":"table_schema","type":
{"id":"63335ce0-d1a0-11ea-87d0-000000000009","value":"VARCHAR"},
"length":null,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":3,"nullable":true},
{"id":"63335ce0-d1a0-11ea-87d0-000000000010","mnemonic":"table_name","type":
{"id":"63335ce0-d1a0-11ea-87d0-000000000011","value":"VARCHAR"},
"length":null,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":4,"nullable":true},
{"id":"63335ce0-d1a0-11ea-87d0-000000000012","mnemonic":"constraint_type","type":
{"id":"63335ce0-d1a0-11ea-87d0-000000000013","value":"VARCHAR"},
"length":null,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":5,"nullable":true}
],"primaryKeys":null}');
-- rollback DELETE FROM system_views_registry where name='TABLE_CONSTRAINTS';

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

-- changeset dkosiakov::add_key_column_usage_to_system_views_registry
INSERT INTO system_views_registry (name, query, `schema`)
VALUES ('KEY_COLUMN_USAGE',
        'SELECT null AS constraint_catalog, datamart_mnemonics AS constraint_schema,
         CONCAT(datamart_mnemonics,''_'',''entity_mnemonics'',''_'',''pk'') AS constraint_name, datamart_mnemonics AS table_schema, entity_mnemonics AS table_name, attr.attr_mnemonics AS column_name, attr.primary_key_order AS ordinal_position
         FROM entities_registry entity
         INNER JOIN datamarts_registry datamarts ON datamarts.datamart_id = entity.datamart_id
         INNER JOIN attributes_registry attr ON attr.entity_id = entity.entity_id
         WHERE attr.primary_key_order IS NOT NULL
         UNION ALL
         SELECT null AS constraint_catalog, datamart_mnemonics AS constraint_schema,
         CONCAT(datamart_mnemonics,''_'',''entity_mnemonics'',''_'',''sk'') AS constraint_name, datamart_mnemonics AS table_schema, entity_mnemonics AS table_name, attr.attr_mnemonics AS column_name, attr.distribute_key_order AS ordinal_position
         FROM entities_registry entity
         INNER JOIN datamarts_registry datamarts ON datamarts.datamart_id = entity.datamart_id
         INNER JOIN attributes_registry attr ON attr.entity_id = entity.entity_id
         WHERE attr.distribute_key_order IS NOT NULL',
'{"id":"63335ce0-d1a0-11ea-87d0-000000000001","mnemonic":"KEY_COLUMN_USAGE","datamartMnemonic":"information_schema","label":"KEY_COLUMN_USAGE",
"tableAttributes":[
{"id":"63335ce0-d1a0-11ea-87d0-000000000002","mnemonic":"constraint_catalog","type":
{"id":"63335ce0-d1a0-11ea-87d0-000000000003","value":"VARCHAR"},
"length":null,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":0,"nullable":true},
{"id":"63335ce0-d1a0-11ea-87d0-000000000004","mnemonic":"constraint_schema","type":
{"id":"63335ce0-d1a0-11ea-87d0-000000000005","value":"VARCHAR"},
"length":null,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":1,"nullable":true},
{"id":"63335ce0-d1a0-11ea-87d0-000000000006","mnemonic":"constraint_name","type":
{"id":"63335ce0-d1a0-11ea-87d0-000000000007","value":"VARCHAR"},
"length":null,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":2,"nullable":true},
{"id":"63335ce0-d1a0-11ea-87d0-000000000008","mnemonic":"table_schema","type":
{"id":"63335ce0-d1a0-11ea-87d0-000000000009","value":"VARCHAR"},
"length":null,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":3,"nullable":true},
{"id":"63335ce0-d1a0-11ea-87d0-000000000010","mnemonic":"table_name","type":
{"id":"63335ce0-d1a0-11ea-87d0-000000000011","value":"VARCHAR"},
"length":null,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":4,"nullable":true},
{"id":"63335ce0-d1a0-11ea-87d0-000000000012","mnemonic":"column_name","type":
{"id":"63335ce0-d1a0-11ea-87d0-000000000013","value":"VARCHAR"},
"length":null,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":5,"nullable":true},
{"id":"63335ce0-d1a0-11ea-87d0-000000000014","mnemonic":"ordinal_position","type":
{"id":"63335ce0-d1a0-11ea-87d0-000000000015","value":"INT"},
"length":null,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":6,"nullable":true}
],"primaryKeys":null}');
-- rollback DELETE FROM system_views_registry where name='KEY_COLUMN_USAGE';
