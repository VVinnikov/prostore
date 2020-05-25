-- configuration migration from stand with version 1.0
delete from attributes_registry where 1 = 1;
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (7, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (7, 4, 'r_obj', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (7, 1, 'iss_by', 32, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (7, 6, 'crt_on', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (7, 6, 'upd_on', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (7, 1, 'iss_id', 6, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (7, 1, 'vrf_stu', 16, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (7, 1, 'gnr', 25, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (7, 1, 'dsc', 10, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (7, 1, 'stu', 1, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (7, 1, 'det_dat', 32, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (7, 6, 'vrf_on', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (7, 1, 'num', 8, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (7, 6, 'exp_dat', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (7, 1, 'iss_plc', 32, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (7, 1, 'ser', 8, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (7, 6, 'iss_da', null, null);
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
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (15, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (15, 4, 'r_obj', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (15, 6, 'iss_da', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (15, 1, 'name', 25, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (16, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (16, 1, 'det_dat', 32, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (16, 1, 'gnr', 25, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (16, 1, 'stu', 1, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (16, 1, 'ser', 8, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (16, 4, 'r_obj', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (16, 1, 'iss_by', 32, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (16, 6, 'exp_dat', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (16, 6, 'iss_da', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (16, 1, 'iss_plc', 32, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (16, 1, 'num', 8, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (16, 6, 'crt_on', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (16, 6, 'upd_on', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (16, 1, 'vrf_stu', 16, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (16, 6, 'vrf_on', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (16, 1, 'dsc', 10, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (16, 1, 'iss_id', 6, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (18, 1, 'gnr', 25, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (18, 1, 'stu', 1, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (18, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (18, 1, 'ser', 8, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (18, 6, 'iss_da', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (18, 1, 'iss_plc', 32, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (18, 1, 'num', 8, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (18, 1, 'det_dat', 32, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (18, 1, 'iss_by', 32, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (18, 6, 'exp_dat', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (18, 6, 'crt_on', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (18, 6, 'upd_on', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (18, 4, 'r_obj', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (18, 1, 'dsc', 10, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (18, 1, 'iss_id', 6, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (18, 1, 'vrf_stu', 16, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (18, 6, 'vrf_on', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (20, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (20, 4, 'r_obj', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (20, 6, 'iss_da', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (20, 1, 'name', 25, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (22, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (22, 1, 'name', 25, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (22, 6, 'iss_da', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (22, 4, 'r_obj', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (23, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (23, 1, 'table_field', 1, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (24, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (24, 1, 'table_field', 1, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (28, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (28, 1, 'table_field', 1, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (29, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (29, 1, 'table_field', 1, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (30, 1, 'table_field', 1, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (30, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (31, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (31, 1, 'table_field', 1, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (32, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (32, 1, 'table_field', 1, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (40, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (40, 1, 'name', 25, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (40, 6, 'iss_da', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (40, 4, 'r_obj', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (41, 4, 'r_obj', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (41, 6, 'iss_da', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (41, 1, 'name', 25, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (41, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (42, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (42, 6, 'iss_da', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (42, 1, 'name', 25, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (42, 4, 'r_obj', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (43, 1, 'name', 25, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (43, 4, 'r_obj', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (43, 6, 'iss_da', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (43, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (44, 6, 'iss_da', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (44, 1, 'name', 25, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (44, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (44, 4, 'r_obj', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (46, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (46, 1, 'name', 25, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (46, 6, 'iss_da', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (46, 4, 'r_obj', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (47, 6, 'iss_da', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (47, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (47, 1, 'name', 25, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (47, 4, 'r_obj', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (48, 1, 'name', 25, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (48, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (48, 6, 'iss_da', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (48, 4, 'r_obj', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (49, 4, 'id', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (49, 4, 'r_obj', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (49, 1, 'name', 25, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (49, 6, 'iss_da', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (50, 6, 'iss_da', null, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (50, 4, 'r_obj', 11, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (50, 1, 'name', 25, null);
INSERT INTO attributes_registry (entity_id, data_type_id, attr_mnemonics, length, accuracy) VALUES (50, 4, 'id', 11, null);

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
