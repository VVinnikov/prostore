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
