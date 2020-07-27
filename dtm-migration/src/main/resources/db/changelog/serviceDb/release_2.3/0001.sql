-- changeset ilapa:add_ordinal_position_to_attributes_registry
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.COLUMNS where table_name = 'attributes_registry' and table_schema = database() and column_name = 'ordinal_position';
alter table attributes_registry
    add ordinal_position int default 0 not null;

-- changeset ilapa:add_nullable_to_attributes_registry
-- preconditions onFail:MARK_RAN
-- precondition-sql-check expectedResult:0 select count(*) from information_schema.COLUMNS where table_name = 'attributes_registry' and table_schema = database() and column_name = 'nullable';
alter table attributes_registry
    add nullable boolean default true null;

-- changeset ilapa:update_nullable_and_ordinal_position_in_attributes_registry
update attributes_registry a
set nullable = (primary_key_order is null), ordinal_position = attr_id - (select min(attr_id) from attributes_registry min_attr_id where min_attr_id.entity_id = a.entity_id);
