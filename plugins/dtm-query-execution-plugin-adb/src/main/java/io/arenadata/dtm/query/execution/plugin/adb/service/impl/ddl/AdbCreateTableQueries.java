package io.arenadata.dtm.query.execution.plugin.adb.service.impl.ddl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityFieldUtils;
import io.arenadata.dtm.common.model.ddl.EntityTypeUtil;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import lombok.val;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class AdbCreateTableQueries {

    public static final String ACTUAL_TABLE_POSTFIX = "actual";
    /**
     * History table name
     */
    public static final String HISTORY_TABLE_POSTFIX = "history";
    /**
     * Name staging table
     */
    public static final String STAGING_TABLE_POSTFIX = "staging";
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
    public static final String TABLE_POSTFIX_DELIMITER = "_";

    private final String ACTUAL_TABLE_QUERY;
    private final String HISTORY_TABLE_QUERY;
    private final String STAGING_TABLE_QUERY;

    public AdbCreateTableQueries(DdlRequestContext context) {
        Entity entity = context.getRequest().getEntity();
        ACTUAL_TABLE_QUERY = createTableQuery(entity, getTableName(entity, ACTUAL_TABLE_POSTFIX), false, false);
        HISTORY_TABLE_QUERY = createTableQuery(entity, getTableName(entity, HISTORY_TABLE_POSTFIX), false, true);
        STAGING_TABLE_QUERY = createTableQuery(entity, getTableName(entity, STAGING_TABLE_POSTFIX), true, false);
    }

    public String getCreateActualTableQuery() {
        return ACTUAL_TABLE_QUERY;
    }

    public String getCreateHistoryTableQuery() {
        return HISTORY_TABLE_QUERY;
    }

    public String getCreateStagingTableQuery() {
        return STAGING_TABLE_QUERY;
    }

    private String getTableName(Entity entity,
                                String tablePostfix) {
        return entity.getNameWithSchema() + TABLE_POSTFIX_DELIMITER + tablePostfix;
    }

    private String createTableQuery(Entity entity,
                                    String tableName,
                                    boolean addReqId,
                                    boolean pkWithSystemFields) {
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

    private void appendPrimaryKeys(StringBuilder builder,
                                   String tableName,
                                   Collection<EntityField> pkList,
                                   boolean addSystemFields) {
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

    private void appendShardingKeys(StringBuilder builder,
                                    Collection<EntityField> skList) {
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

    private void appendSystemColumns(StringBuilder builder,
                                     String delimiter) {
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
}
