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
    public String createTableScripts(Entity entity) {
        return new StringBuilder()
                .append(createTableScript(entity, entity.getNameWithSchema()
                        + TABLE_POSTFIX_DELIMITER + ACTUAL_TABLE, false, false))
                .append(QUERY_DELIMITER)
                .append(createTableScript(entity, entity.getNameWithSchema()
                        + TABLE_POSTFIX_DELIMITER + HISTORY_TABLE, false, true))
                .append(QUERY_DELIMITER)
                .append(createTableScript(entity, entity.getNameWithSchema()
                        + TABLE_POSTFIX_DELIMITER + STAGING_TABLE, true, false))
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

    private String createTableScript(Entity entity, String tableName, boolean addReqId, boolean pkWithSystemFields) {
        val initDelimiter = entity.getFields().isEmpty() ? " " : DELIMITER;
        val sb = new StringBuilder()
                .append("CREATE TABLE ").append(tableName)
                .append(" (");
        appendClassTableFields(sb, entity.getFields());
        appendSystemColumns(sb, initDelimiter);
        if (addReqId) {
            appendReqIdColumn(sb);
        }
        List<EntityField> pkList = EntityFieldUtils.getPrimaryKeyList(entity.getFields());
        if (pkWithSystemFields || pkList.size() > 0) {
            appendPrimaryKeys(sb, tableName, pkList, pkWithSystemFields);
        }
        sb.append(")");
        val shardingKeyList = EntityFieldUtils.getShardingKeyList(entity.getFields());
        if (shardingKeyList.size() > 0) {
            appendShardingKeys(sb, shardingKeyList);
        }
        return sb.toString();
    }

    private void appendClassTableFields(StringBuilder builder, List<EntityField> fields) {
        val columns = fields.stream()
                .map(this::getColumnDDLByField)
                .collect(Collectors.joining(DELIMITER));
        builder.append(columns);
    }

    private String getColumnDDLByField(EntityField field) {
        val sb = new StringBuilder();
        sb.append(field.getName())
                .append(" ")
                .append(EntityTypeUtil.pgFromDtmType(field))
                .append(" ");
        if (!field.getNullable()) {
            sb.append("NOT NULL");
        }
        return sb.toString();
    }

    private void appendPrimaryKeys(StringBuilder builder, String tableName, Collection<EntityField> pkList, boolean addSystemFields) {
        List<String> pkFields = pkList.stream().map(EntityField::getName).collect(Collectors.toList());
        if (addSystemFields) {
            pkFields.add(SYS_FROM_ATTR);
        }
        builder.append(DELIMITER)
                .append("constraint ")
                .append("pk_")
                .append(tableName.replace('.', '_'))
                .append(" primary key (")
                .append(pkFields.stream().collect(Collectors.joining(DELIMITER)))
                .append(")");
    }

    private void appendShardingKeys(StringBuilder builder, Collection<EntityField> skList) {
        builder.append(" DISTRIBUTED BY (")
                .append(skList.stream().map(EntityField::getName).collect(Collectors.joining(DELIMITER)))
                .append(")");
    }

    private void appendReqIdColumn(StringBuilder builder) {
        builder.append(DELIMITER)
                .append(REQ_ID_ATTR)
                .append(" ")
                .append("varchar(36)");
    }

    private void appendSystemColumns(StringBuilder builder, String delimiter) {
        builder.append(delimiter)
                .append(SYS_FROM_ATTR)
                .append(" ")
                .append("bigint")
                .append(DELIMITER)
                .append(SYS_TO_ATTR)
                .append(" ")
                .append("bigint")
                .append(DELIMITER)
                .append(SYS_OP_ATTR)
                .append(" ")
                .append("int");
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