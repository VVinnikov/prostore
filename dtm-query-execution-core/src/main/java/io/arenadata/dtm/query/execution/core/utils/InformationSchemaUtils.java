package io.arenadata.dtm.query.execution.core.utils;

public class InformationSchemaUtils {

    public static final String LOGIC_SCHEMA_KEY_COLUMN_USAGE =
            "CREATE VIEW IF NOT EXISTS DTM.logic_schema_key_column_usage AS \n" +
                    "SELECT constraint_catalog, constraint_schema, constraint_name, table_schema, table_name, column_name, ordinal_position\n" +
                    "FROM information_schema.KEY_COLUMN_USAGE\n" +
                    "WHERE constraint_schema NOT IN ('DTM', 'INFORMATION_SCHEMA', 'SYSTEM_LOBS')";
    public static final String LOGIC_SCHEMA_DATAMARTS =
            "CREATE VIEW IF NOT EXISTS DTM.logic_schema_datamarts AS \n" +
                    "SELECT catalog_name, schema_name\n" +
                    "FROM information_schema.schemata \n" +
                    "WHERE schema_name NOT IN ('DTM', 'INFORMATION_SCHEMA', 'SYSTEM_LOBS')";
    public static final String LOGIC_SCHEMA_ENTITIES =
            "CREATE VIEW IF NOT EXISTS DTM.logic_schema_entities AS \n" +
                    "SELECT table_catalog, table_schema, table_name, table_type\n" +
                    "FROM information_schema.tables \n" +
                    "WHERE table_schema not in ('DTM', 'INFORMATION_SCHEMA', 'SYSTEM_LOBS')";
    public static final String LOGIC_SCHEMA_COLUMNS =
            "CREATE VIEW IF NOT EXISTS DTM.logic_schema_columns AS\n" +
                    "SELECT table_catalog, table_schema, table_name, column_name, is_nullable, character_maximum_length, datetime_precision,\n" +
                    "  case \n" +
                    "    when data_type = 'DOUBLE PRECISION' then 'DOUBLE' \n" +
                    "    when data_type = 'CHARACTER VARYING' then 'VARCHAR' \n" +
                    "    when data_type = 'INTEGER' then 'INT' \n" +
                    "    else data_type \n" +
                    "  end as data_type   \n" +
                    "FROM information_schema.COLUMNS\n" +
                    "WHERE table_schema NOT IN ('DTM', 'INFORMATION_SCHEMA', 'SYSTEM_LOBS')";
    public static final String LOGIC_SCHEMA_ENTITY_CONSTRAINTS =
            "CREATE VIEW IF NOT EXISTS DTM.logic_schema_entity_constraints AS\n" +
                    "SELECT kcu.constraint_catalog,\n" +
                    "       kcu.constraint_schema,\n" +
                    "       si.index_name as constraint_name,\n" +
                    "       kcu.table_schema,\n" +
                    "       kcu.table_name,\n" +
                    "       case\n" +
                    "           when si.INDEX_NAME like 'SK_%' then 'sharding key'\n" +
                    "           when si.INDEX_NAME like '%_PK_%' then 'primary key'\n" +
                    "           ELSE '-'\n" +
                    "           end       AS CONSTRAINT_TYPE\n" +
                    "FROM information_schema.KEY_COLUMN_USAGE kcu,\n" +
                    "     information_schema.SYSTEM_INDEXSTATS si\n" +
                    "WHERE kcu.CONSTRAINT_CATALOG = si.TABLE_CATALOG\n" +
                    "  and kcu.TABLE_CATALOG = si.TABLE_CATALOG\n" +
                    "  and kcu.TABLE_SCHEMA = si.TABLE_SCHEMA\n" +
                    "  and kcu.TABLE_NAME = si.TABLE_NAME\n" +
                    "  and kcu.constraint_schema NOT IN ('DTM', 'INFORMATION_SCHEMA', 'SYSTEM_LOBS')";
    public static final String CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS %s";

    public static final String CREATE_SHARDING_KEY_INDEX = "CREATE INDEX IF NOT EXISTS sk_%s on %s (%s)";

    public static final String COMMENT_ON_COLUMN = "COMMENT ON COLUMN %s.%s IS '%s'";
}
