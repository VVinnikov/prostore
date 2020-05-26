insert into datamarts_registry(datamart_id, datamart_mnemonics) values (1, 'test_datamart');
commit;

insert into entities_registry(entity_id, datamart_id, entity_mnemonics) values (1, 1, 'PSO');
insert into entities_registry(entity_id, datamart_id, entity_mnemonics) values (2, 1, 'DOC');
insert into entities_registry(entity_id, datamart_id, entity_mnemonics) values (3, 1, 'OBJ');
insert into entities_registry(entity_id, datamart_id, entity_mnemonics) values (4, 1, 'REG_CXT');
commit;

insert into data_types_registry(data_type_id, data_type_mnemonics) values (1, 'varchar');
insert into data_types_registry(data_type_id, data_type_mnemonics) values (2, 'bigint');
insert into data_types_registry(data_type_id, data_type_mnemonics) values (3, 'datetime');
insert into data_types_registry(data_type_id, data_type_mnemonics) values (4, 'int');
commit;

insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (1, 1,  2, 'ID');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (2, 1,  1, 'LST_NAM', 20);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (3, 1,  1, 'FST_NAM', 20);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (4, 1,  1, 'MID_NAM', 20);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (5, 1,  1, 'OTR_NAM', 20);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (6, 1,  3, 'BRD');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (7, 1,  1, 'GNR', 1);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (8, 1,  4, 'SNILS', 9000000);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (9, 1,  4, 'INN', 9000000);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (10, 1,  1, 'TSD', 1);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (11, 1,  1, 'NON_RSD_ID', 8);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (12, 1,  1, 'PHY');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (13, 1,  2, 'R_ORG');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (14, 1,  1, 'PRD_TMZ', 8);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (15, 1,  1, 'PRD_LAN', 3);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (16, 1,  1, 'R_CTZ_SHP');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (17, 1,  1, 'R_CTY');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (18, 1,  1, 'STU', 1);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (19, 1,  1, 'BSS', 8);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (20, 1,  1, 'BRD_PLC', 8);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (21, 1,  4, 'INDEX', 900000);

insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (22, 2,  2, 'ID');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (23, 2,  1, 'GNR');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (24, 2,  1, 'STU', 1);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (25, 2,  1, 'SER', 8);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (26, 2,  1, 'NUM', 8);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (27, 2,  1, 'ISS_PLC', 32);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (28, 2,  3, 'EXP_DAT');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (29, 2,  1, 'DET_DAT', 32);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (30, 2,  2, 'R_OBJ');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (31, 2,  3, 'CRT_ON');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (32, 2,  3, 'UPD_ON');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (33, 2,  1, 'ISS_ID', 6);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (34, 2,  1, 'VRF_STU', 16);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (35, 2,  3, 'VRF_ON');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (36, 2,  1, 'ISS_BY', 32);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (37, 2,  3, 'ISS_DA');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (38, 2,  1, 'DSC');

insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (39, 3,  3, 'CRT_ON');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (40, 3,  3, 'UPD_ON');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (41, 3,  1, 'DSC', 32);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (42, 3,  4, 'OID', 100);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (43, 3,  1, 'VRF_ST', 32);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (44, 3,  1, 'CTX', 32);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (45, 3,  2, 'ID');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (46, 3,  1, 'OBJ_TYP', 2);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (47, 3,  1, 'FUL_NAM', 32);

insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (48, 4,  3, 'REG_ON');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (49, 4,  1, 'ACT_TYP', 3);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (50, 4,  1, 'ACT_ON');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (51, 4,  1, 'ACT_TILL');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (52, 4,  3, 'CRT_ON');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (53, 4,  3, 'UPD_ON');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (54, 4,  1, 'DSC', 8);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (55, 4,  4, 'R_REG_RA', 100);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (56, 4,  4, 'R_REG_STF_UNT', 100);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (57, 4,  1, 'CFM_TYP', 3);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (58, 4,  1, 'CFM_STE', 7);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (59, 4,  1, 'CFM_STE_MSG', 8);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (60, 4,  3, 'CFM_ON');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (61, 4,  4, 'R_CFM_RA', 100);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (62, 4,  1, 'CFM_CXT');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (63, 4,  1, 'ACT_CXT', 8);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (64, 4,  4, 'CFM_SNILS', 900000);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (65, 4,  4, 'R_CFM_STF_UNT', 100);
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (66, 4,  2, 'ID');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics)
    values (67, 4,  2, 'R_OBJ');
insert into attributes_registry(attr_id, entity_id, data_type_id, attr_mnemonics, length)
    values (68, 4,  1, 'REG_TYP', 3);
