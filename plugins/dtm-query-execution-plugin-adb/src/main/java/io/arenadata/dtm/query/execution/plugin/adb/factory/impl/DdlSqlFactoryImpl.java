package io.arenadata.dtm.query.execution.plugin.adb.factory.impl;

import io.arenadata.dtm.query.execution.plugin.adb.factory.DdlSqlFactory;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collections;

import static io.arenadata.dtm.query.execution.plugin.adb.factory.Constants.*;

@Service("ddlSqlFactoryImpl")
public class DdlSqlFactoryImpl implements DdlSqlFactory {
    public static final String QUERY_DELIMITER = "; ";
    public static final String TABLE_POSTFIX_DELIMITER = "_";

    private static final String DELIMITER = ", ";
    private static final String DROP_TABLE = "DROP TABLE IF EXISTS ";
    private static final String DROP_SCHEMA = "DROP SCHEMA IF EXISTS %s CASCADE";
    private static final String CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS %s";
    private static final String CREATE_INDEX_SQL = "CREATE INDEX %s_%s_%s ON %s.%s_%s (%s)";

    @Override
    public String createDropTableScript(String tableNameWithSchema) {
        return DROP_TABLE + tableNameWithSchema +
                TABLE_POSTFIX_DELIMITER + ACTUAL_TABLE +
                QUERY_DELIMITER +
                DROP_TABLE + tableNameWithSchema +
                TABLE_POSTFIX_DELIMITER + HISTORY_TABLE +
                QUERY_DELIMITER +
                DROP_TABLE + tableNameWithSchema +
                TABLE_POSTFIX_DELIMITER + STAGING_TABLE +
                QUERY_DELIMITER;
    }

    @Override
    public String createSchemaSqlQuery(String schema) {
        return String.format(CREATE_SCHEMA, schema);
    }

    @Override
    public String dropSchemaSqlQuery(String schema) {
        return String.format(DROP_SCHEMA, schema);
    }

    @Override
    public String createSecondaryIndexSqlQuery(String schema, String table) {
        final String idxPostfix = "_idx";
        return String.format(CREATE_INDEX_SQL, table, ACTUAL_TABLE,
                SYS_FROM_ATTR + idxPostfix, schema, table, ACTUAL_TABLE,
                String.join(DELIMITER, Collections.singletonList(SYS_FROM_ATTR))) +
                QUERY_DELIMITER +
                String.format(CREATE_INDEX_SQL, table, HISTORY_TABLE,
                        SYS_TO_ATTR + idxPostfix, schema, table, HISTORY_TABLE,
                        String.join(DELIMITER, Arrays.asList(SYS_TO_ATTR, SYS_OP_ATTR)));
    }
}