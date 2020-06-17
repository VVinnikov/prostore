-- configuration migration from stand with version 1.0
delete from attributes_registry where 1 = 1;
delete from data_types_registry where 1 = 1;
delete from entities_registry where 1 = 1;
commit;

INSERT INTO data_types_registry (data_type_id, data_type_mnemonics) VALUES (1, 'varchar');
INSERT INTO data_types_registry (data_type_id, data_type_mnemonics) VALUES (2, 'bigint');
INSERT INTO data_types_registry (data_type_id, data_type_mnemonics) VALUES (3, 'datetime');
INSERT INTO data_types_registry (data_type_id, data_type_mnemonics) VALUES (4, 'int');
INSERT INTO data_types_registry (data_type_id, data_type_mnemonics) VALUES (5, 'char');
INSERT INTO data_types_registry (data_type_id, data_type_mnemonics) VALUES (6, 'date');
INSERT INTO data_types_registry (data_type_id, data_type_mnemonics) VALUES (7, 'timestamp');
INSERT INTO data_types_registry (data_type_id, data_type_mnemonics) VALUES (8, 'decimal');
INSERT INTO data_types_registry (data_type_id, data_type_mnemonics) VALUES (9, 'numeric');
INSERT INTO data_types_registry (data_type_id, data_type_mnemonics) VALUES (10, 'integer');
INSERT INTO data_types_registry (data_type_id, data_type_mnemonics) VALUES (11, 'float');
INSERT INTO data_types_registry (data_type_id, data_type_mnemonics) VALUES (12, 'double');
INSERT INTO data_types_registry (data_type_id, data_type_mnemonics) VALUES (13, 'boolean');
INSERT INTO data_types_registry (data_type_id, data_type_mnemonics) VALUES (14, 'tinyint');

INSERT INTO entities_registry (entity_id, datamart_id, entity_mnemonics) VALUES (9, 1, 'obj');
INSERT INTO entities_registry (entity_id, datamart_id, entity_mnemonics) VALUES (10, 1, 'reg_cxt');
INSERT INTO entities_registry (entity_id, datamart_id, entity_mnemonics) VALUES (11, 1, 'pso');
INSERT INTO entities_registry (entity_id, datamart_id, entity_mnemonics) VALUES (14, 1, 'doc');

INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (9, 6, 'upd_on', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (9, 4, 'oid', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (9, 1, 'dsc', 32, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (9, 1, 'obj_typ', 2, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (9, 6, 'crt_on', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (9, 1, 'ful_nam', 32, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (9, 1, 'vrf_st', 32, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (9, 1, 'ctx', 32, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (9, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (10, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (10, 1, 'act_till', 10, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (10, 1, 'act_on', 10, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (10, 1, 'act_typ', 3, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (10, 1, 'reg_typ', 3, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (10, 6, 'crt_on', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (10, 1, 'dsc', 8, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (10, 6, 'reg_on', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (10, 4, 'r_reg_ra', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (10, 6, 'upd_on', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (10, 4, 'r_reg_stf_unt', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (10, 1, 'cfm_typ', 3, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (10, 4, 'r_cfm_ra', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (10, 6, 'cfm_on', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (10, 1, 'cfm_ste_msg', 8, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (10, 1, 'cfm_cxt', 10, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (10, 1, 'act_cxt', 8, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (10, 1, 'cfm_ste', 7, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (10, 4, 'cfm_snils', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (10, 6, 'cfm_std_on', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (10, 4, 'r_cfm_stf_unt', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (10, 4, 'r_obj', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (10, 1, 'act_cod', 10, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (11, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (11, 1, 'mid_nam', 1024, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (11, 5, 'tsd', 1, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (11, 1, 'otr_nam', 1024, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (11, 1, 'non_rsd_id', 8, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (11, 1, 'lst_nam', 1024, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (11, 4, 'r_org', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (11, 5, 'gnr', 1, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (11, 4, 'snils', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (11, 1, 'prd_lan', 3, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (11, 1, 'prd_tmz', 3, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (11, 6, 'brd', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (11, 4, 'inn', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (11, 1, 'phy', 1024, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (11, 5, 'stu', 1, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (11, 4, 'r_id_doc', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (11, 1, 'r_ctz_shp', 3, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (11, 1, 'brd_plc', 8, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (11, 1, 'fst_nam', 1024, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (11, 1, 'bss', 8, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (11, 1, 'r_cty', 3, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (11, 4, 'index', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (14, 1, 'stu', 1, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (14, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (14, 1, 'gnr', 25, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (14, 1, 'det_dat', 32, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (14, 1, 'ser', 8, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (14, 1, 'num', 8, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (14, 1, 'iss_by', 32, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (14, 6, 'iss_da', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (14, 1, 'iss_plc', 32, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (14, 6, 'exp_dat', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (14, 6, 'upd_on', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (14, 4, 'r_obj', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (14, 1, 'vrf_stu', 16, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (14, 1, 'iss_id', 6, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (14, 6, 'crt_on', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (14, 1, 'dsc', 10, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (14, 6, 'vrf_on', null, null);
-- migrate data types
-- tinyint => int
update attributes_registry ar
    set data_type_id = (select data_type_id from data_types_registry where data_type_mnemonics = 'int')
    where data_type_id = (select data_type_id from data_types_registry where data_type_mnemonics = 'tinyint');
-- integer => int
update attributes_registry ar
    set data_type_id = (select data_type_id from data_types_registry where data_type_mnemonics = 'int')
    where data_type_id = (select data_type_id from data_types_registry where data_type_mnemonics = 'integer');
-- numeric => double
update attributes_registry ar
    set data_type_id = (select data_type_id from data_types_registry where data_type_mnemonics = 'double')
    where data_type_id = (select data_type_id from data_types_registry where data_type_mnemonics = 'numeric');
-- decimal => double
update attributes_registry ar
    set data_type_id = (select data_type_id from data_types_registry where data_type_mnemonics = 'double')
    where data_type_id = (select data_type_id from data_types_registry where data_type_mnemonics = 'decimal');
-- datetime => timestamp
update attributes_registry ar
    set data_type_id = (select data_type_id from data_types_registry where data_type_mnemonics = 'timestamp')
    where data_type_id = (select data_type_id from data_types_registry where data_type_mnemonics = 'datetime');

-- delete obsolete data types
delete from data_types_registry
    where data_type_mnemonics in ('tinyint','integer','numeric','decimal','datetime');

-- add new data types
insert into data_types_registry(data_type_mnemonics) values ('uuid');
insert into data_types_registry(data_type_mnemonics) values ('time');
