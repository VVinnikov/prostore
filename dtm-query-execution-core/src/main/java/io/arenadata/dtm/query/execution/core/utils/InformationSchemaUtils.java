package io.arenadata.dtm.query.execution.core.utils;

public class InformationSchemaUtils {

    public static final String LOGIC_SCHEMA_KEY_COLUMN_USAGE =
            "CREATE VIEW IF NOT EXISTS DTM.logic_schema_key_column_usage AS \n" +
                    "SELECT constraint_catalog, constraint_schema, constraint_name, table_schema, table_name, column_name, ordinal_position\n" +
                    "FROM information_schema.KEY_COLUMN_USAGE\n" +
                    "WHERE constraint_catalog = 'dtm'";
    public static final String LOGIC_SCHEMA_DATAMARTS =
            "CREATE VIEW IF NOT EXISTS DTM.logic_schema_datamarts AS \n" +
                    "SELECT catalog_name, schema_name\n" +
                    "FROM information_schema.schemata \n" +
                    "WHERE catalog_name = 'dtm'";
    public static final String LOGIC_SCHEMA_ENTITIES =
            "CREATE VIEW IF NOT EXISTS DTM.logic_schema_entities AS \n" +
                    "SELECT table_catalog, table_schema, table_name, table_type\n" +
                    "FROM information_schema.tables \n" +
                    "WHERE table_catalog = 'dtm'";
    public static final String LOGIC_SCHEMA_ATTRIBUTES =
            "CREATE VIEW IF NOT EXISTS DTM.logic_schema_attributes AS\n" +
                    "SELECT table_catalog, table_schema, table_name, column_name, is_nullable, character_maximum_length, datetime_precision,\n" +
                    "  case \n" +
                    "    when data_type = 'DOUBLE PRECISION' then 'DOUBLE' \n" +
                    "    when data_type = 'CHARACTER VARYING' then 'VARCHAR' \n" +
                    "    when data_type = 'INTEGER' then 'INT' \n" +
                    "    else data_type \n" +
                    "  end as data_type   \n" +
                    "FROM information_schema.COLUMNS\n" +
                    "WHERE table_catalog = 'dtm'";
    public static final String LOGIC_SCHEMA_ENTITY_CONSTRAINTS =
            "CREATE VIEW IF NOT EXISTS DTM.logic_schema_entity_constraints AS\n" +
                    "    SELECT constraint_catalog, constraint_schema, constraint_name, table_schema, table_name, 'primary key' AS CONSTRAINT_TYPE\n" +
                    "    FROM information_schema.KEY_COLUMN_USAGE\n" +
                    "    WHERE table_catalog = 'dtm' AND constraint_name = 'PRIMARY'\n" +
                    "  UNION ALL\n" +
                    "    SELECT constraint_catalog, constraint_schema, constraint_name, table_schema, table_name, 'sharding key' AS CONSTRAINT_TYPE\n" +
                    "    FROM information_schema.KEY_COLUMN_USAGE\n" +
                    "    WHERE table_catalog = 'dtm' AND constraint_name like '%_sk_%'";

    public static final String CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS %s";

    public static final String CREATE_SHARDING_KEY_INDEX = "CREATE INDEX IF NOT EXISTS sk_%s on %s (%s)";

    public static final String COMMENT_ON_COLUMN = "COMMENT ON COLUMN %s.%s IS '%s'";
}
