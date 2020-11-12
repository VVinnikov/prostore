package io.arenadata.dtm.query.execution.plugin.adb.factory.impl;

import io.arenadata.dtm.common.model.ddl.*;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import lombok.val;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Component
public class MetadataSqlFactoryImpl implements MetadataSqlFactory {
    /**
     * Name of the table of actual data
     */
    public static final String ACTUAL_TABLE = "actual";
    /**
     * History table name
     */
    public static final String HISTORY_TABLE = "history";
    /**
     * Name staging table
     */
    public static final String STAGING_TABLE = "staging";
    /**
     * Delta Number System Field
     */
    public static final String SYS_FROM_ATTR = "sys_from";
    /**
     * System field of maximum delta number
     */
    public static final String SYS_TO_ATTR = "sys_to";
    /**
     * System field of operation on an object
     */
    public static final String SYS_OP_ATTR = "sys_op";
    /**
     * Request ID system field
     */
    public static final String REQ_ID_ATTR = "req_id";
    private static final String DELIMITER = ", ";
    private static final String DROP_TABLE = "DROP TABLE IF EXISTS ";
    private static final String DROP_SCHEMA = "DROP SCHEMA IF EXISTS %s CASCADE";
    private static final String CREATE_SCHEMA = "CREATE SCHEMA %s";
    private static final String KEY_COLUMNS_TEMPLATE_SQL = "SELECT c.column_name, c.data_type\n" +
            "FROM information_schema.table_constraints tc\n" +
            "         JOIN information_schema.KEY_COLUMN_USAGE AS ccu USING (constraint_schema, constraint_name)\n" +
            "         JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema\n" +
            "    AND tc.table_name = c.table_name AND ccu.column_name = c.column_name\n" +
            "WHERE constraint_type = 'PRIMARY KEY'\n" +
            "  and c.table_schema = '%s' and tc.table_name = '%s'";
    private static final String CREATE_INDEX_SQL = "CREATE INDEX %s_%s_%s ON %s.%s_%s (%s)";
    public static final String QUERY_DELIMITER = "; ";
    public static final String TABLE_POSTFIX_DELIMITER = "_";

    @Override
    public String createDropTableScript(Entity entity) {
        return new StringBuilder()
                .append(DROP_TABLE).append(entity.getNameWithSchema())
                .append(TABLE_POSTFIX_DELIMITER).append(ACTUAL_TABLE)
                .append(QUERY_DELIMITER)
                .append(DROP_TABLE).append(entity.getNameWithSchema())
                .append(TABLE_POSTFIX_DELIMITER).append(HISTORY_TABLE)
                .append(QUERY_DELIMITER)
                .append(DROP_TABLE).append(entity.getNameWithSchema())
                .append(TABLE_POSTFIX_DELIMITER).append(STAGING_TABLE)
                .append(QUERY_DELIMITER)
                .toString();
    }

    @Override
    public String createSchemaSqlQuery(String schemaName) {
        return String.format(CREATE_SCHEMA, schemaName);
    }

    @Override
    public String dropSchemaSqlQuery(String schemaName) {
        return String.format(DROP_SCHEMA, schemaName);
    }

    @Override
    public String createKeyColumnsSqlQuery(String schema, String tableName) {
        return String.format(KEY_COLUMNS_TEMPLATE_SQL, schema, tableName + TABLE_POSTFIX_DELIMITER + ACTUAL_TABLE);
    }

    @Override
    public String createSecondaryIndexSqlQuery(String schema, String tableName) {
        StringBuilder sb = new StringBuilder();
        final String idxPostfix = "_idx";
        sb.append(String.format(CREATE_INDEX_SQL, tableName, ACTUAL_TABLE,
                SYS_FROM_ATTR + idxPostfix, schema, tableName, ACTUAL_TABLE,
                String.join(DELIMITER, Collections.singletonList(SYS_FROM_ATTR))));
        sb.append(QUERY_DELIMITER);
        sb.append(String.format(CREATE_INDEX_SQL, tableName, HISTORY_TABLE,
                SYS_TO_ATTR + idxPostfix, schema, tableName, HISTORY_TABLE,
                String.join(DELIMITER, Arrays.asList(SYS_TO_ATTR, SYS_OP_ATTR))));
        return sb.toString();
    }

    @Override
    public List<ColumnMetadata> createKeyColumnQueryMetadata() {
        List<ColumnMetadata> metadata = new ArrayList<>();
        metadata.add(new ColumnMetadata("column_name", ColumnType.VARCHAR));
        metadata.add(new ColumnMetadata("data_type", ColumnType.VARCHAR));
        return metadata;
    }
}
