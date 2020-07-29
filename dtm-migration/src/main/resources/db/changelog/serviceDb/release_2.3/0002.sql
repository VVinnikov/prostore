-- changeset ilapa:add_table_system_views_registry
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.TABLES where table_name = 'system_views_registry' and table_schema = database();
create table system_views_registry
(
	name varchar(100) not null
		primary key,
	query varchar(4096) null,
	`schema` varchar(4096) null
);

-- changeset ilapa:add_views_to_system_views_registry
INSERT INTO system_views_registry (name, query, `schema`) VALUES ('SCHEMES', 'select NULL AS `catalog_name`, `datamarts_registry`.`datamart_mnemonics` AS `schema_name`
from `datamarts_registry`', '{"id":"f0c0d0d4-cdbb-4fb2-b206-000000000001","mnemonic":"SCHEMES","datamartMnemonic":"information_schema","label":"SCHEMES","tableAttributes":[{"id":"f0c0d0d4-cdbb-4fb2-b206-000000000002","mnemonic":"catalog_name","type":{"id":"f0c0d0d4-cdbb-4fb2-b206-000000000003","value":"VARCHAR"},"length":null,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":0,"nullable":true},{"id":"f0c0d0d4-cdbb-4fb2-b206-000000000004","mnemonic":"schema_name","type":{"id":"f0c0d0d4-cdbb-4fb2-b206-000000000005","value":"VARCHAR"},"length":30,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":1,"nullable":null}],"primaryKeys":null}');
INSERT INTO system_views_registry (name, query, `schema`) VALUES ('DELTAS', 'select NULL                              AS `delta_catalog`,
       `delta_data`.`datamart_mnemonics` AS `delta_schema`,
       `delta_data`.`load_id`            AS `load_id`,
       `delta_data`.`sys_date`           AS `sys_date`,
       `delta_data`.`sin_id`             AS `sin_id`,
       `delta_data`.`status`             AS `status`,
       `delta_data`.`status_date`        AS `status_date`
from `delta_data`', '{"id":"f0c0d0d4-cdbb-4fb2-0001-000000000001","mnemonic":"DELTAS","datamartMnemonic":"information_schema","label":"DELTAS","tableAttributes":[{"id":"f0c0d0d4-cdbb-4fb2-0001-000000000002","mnemonic":"delta_catalog","type":{"id":"f0c0d0d4-cdbb-4fb2-0001-000000000003","value":"VARCHAR"},"length":1024,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":0,"nullable":true},{"id":"f0c0d0d4-cdbb-4fb2-0001-000000000004","mnemonic":"delta_schema","type":{"id":"f0c0d0d4-cdbb-4fb2-0001-000000000005","value":"VARCHAR"},"length":1024,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":1,"nullable":null},{"id":"f0c0d0d4-cdbb-4fb2-0001-000000000006","mnemonic":"load_id","type":{"id":"f0c0d0d4-cdbb-4fb2-0001-000000000007","value":"BIGINT"},"length":20,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":2,"nullable":null},{"id":"f0c0d0d4-cdbb-4fb2-0001-000000000008","mnemonic":"sys_date","type":{"id":"f0c0d0d4-cdbb-4fb2-0001-000000000009","value":"TIMESTAMP"},"length":null,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":3,"nullable":null},{"id":"f0c0d0d4-cdbb-4fb2-0001-000000000010","mnemonic":"sin_id","type":{"id":"f0c0d0d4-cdbb-4fb2-0001-000000000011","value":"BIGINT"},"length":20,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":4,"nullable":null},{"id":"f0c0d0d4-cdbb-4fb2-0001-000000000012","mnemonic":"status","type":{"id":"f0c0d0d4-cdbb-4fb2-0001-000000000013","value":"INT"},"length":11,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":5,"nullable":null},{"id":"f0c0d0d4-cdbb-4fb2-0001-000000000014","mnemonic":"status_date","type":{"id":"f0c0d0d4-cdbb-4fb2-0001-000000000015","value":"TIMESTAMP"},"length":null,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":6,"nullable":null}],"primaryKeys":null}');
INSERT INTO system_views_registry (name, query, `schema`) VALUES ('TABLES', 'select NULL                             AS `table_catalog`,
       `datamarts`.`datamart_mnemonics` AS `table_schema`,
       `entities`.`entity_mnemonics`    AS `table_name`,
       ''BASE TABLE''                     AS `table_type`
from (`entities_registry` `entities`
         join `datamarts_registry` `datamarts` on (`datamarts`.`datamart_id` = `entities`.`datamart_id`))', '{"id":"f0c0d0d4-cdbb-4fb2-0002-000000000001","mnemonic":"TABLES","datamartMnemonic":"information_schema","label":"TABLES","tableAttributes":[{"id":"f0c0d0d4-cdbb-4fb2-0002-000000000002","mnemonic":"table_catalog","type":{"id":"f0c0d0d4-cdbb-4fb2-0002-000000000003","value":"VARCHAR"},"length":1024,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":0,"nullable":true},{"id":"f0c0d0d4-cdbb-4fb2-0002-000000000004","mnemonic":"table_schema","type":{"id":"f0c0d0d4-cdbb-4fb2-0002-000000000005","value":"VARCHAR"},"length":1024,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":1,"nullable":null},{"id":"f0c0d0d4-cdbb-4fb2-0002-000000000006","mnemonic":"table_name","type":{"id":"f0c0d0d4-cdbb-4fb2-0002-000000000007","value":"VARCHAR"},"length":1024,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":2,"nullable":null},{"id":"f0c0d0d4-cdbb-4fb2-0002-000000000008","mnemonic":"table_type","type":{"id":"f0c0d0d4-cdbb-4fb2-0002-000000000009","value":"VARCHAR"},"length":10,"accuracy":null,"primaryKeyOrder":null,"distributeKeyOrder":null,"ordinalPosition":3,"nullable":null}],"primaryKeys":null}');
