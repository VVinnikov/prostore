package ru.ibs.dtm.query.execution.plugin.adb.factory.impl;

import lombok.val;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassFieldUtils;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.model.ddl.ClassTypeUtil;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

@Component
public class MetadataSqlFactoryImpl implements MetadataSqlFactory {
    /**
     * Name of the table of actual data
     */
    public final static String ACTUAL_TABLE = "actual";
    /**
     * History table name
     */
    public final static String HISTORY_TABLE = "history";
    /**
     * Name staging table
     */
    public final static String STAGING_TABLE = "staging";
    /**
     * Delta Number System Field
     */
    public final static String SYS_FROM_ATTR = "sys_from";
    /**
     * System field of maximum delta number
     */
    public final static String SYS_TO_ATTR = "sys_to";
    /**
     * System field of operation on an object
     */
    public final static String SYS_OP_ATTR = "sys_op";
    /**
     * Request ID system field
     */
    public final static String REQ_ID_ATTR = "req_id";
    public static final String DELIMITER = ", ";
    private final static String DROP_TABLE = "DROP TABLE IF EXISTS ";

    @Override
    public String createDropTableScript(ClassTable classTable) {
        return new StringBuilder()
                .append(DROP_TABLE).append(classTable.getNameWithSchema()).append("_").append(ACTUAL_TABLE)
                .append("; ")
                .append(DROP_TABLE).append(classTable.getNameWithSchema()).append("_").append(HISTORY_TABLE)
                .append("; ")
                .append(DROP_TABLE).append(classTable.getNameWithSchema()).append("_").append(STAGING_TABLE)
                .append("; ")
                .toString();
    }

    @Override
    public String createTableScripts(ClassTable classTable) {
        return new StringBuilder()
                .append(createTableScript(classTable, classTable.getNameWithSchema() + "_" + ACTUAL_TABLE, false))
                .append("; ")
                .append(createTableScript(classTable, classTable.getNameWithSchema() + "_" + HISTORY_TABLE, false))
                .append("; ")
                .append(createTableScript(classTable, classTable.getNameWithSchema() + "_" + STAGING_TABLE, true))
                .append("; ")
                .toString();
    }

    private String createTableScript(ClassTable classTable, String tableName, boolean addReqId) {
        val initDelimiter = classTable.getFields().isEmpty() ? " " : DELIMITER;
        val sb = new StringBuilder()
                .append("CREATE TABLE ").append(tableName)
                .append(" (");
        appendClassTableFields(sb, classTable.getFields());
        appendSystemColumns(sb, initDelimiter);
        if (addReqId) {
            appendReqIdColumn(sb);
        }
        val pkList = ClassFieldUtils.getPrimaryKeyList(classTable.getFields());
        if (pkList.size() > 0) {
            appendPrimaryKeys(sb, tableName, pkList);
        }
        sb.append(")");
        val shardingKeyList = ClassFieldUtils.getShardingKeyList(classTable.getFields());
        if (shardingKeyList.size() > 0) {
            appendShardingKeys(sb, shardingKeyList);
        }
        return sb.toString();
    }

    private void appendClassTableFields(StringBuilder builder, List<ClassField> fields) {
        val columns = fields.stream()
                .map(this::getColumnDDLByField)
                .collect(Collectors.joining(DELIMITER));
        builder.append(columns);
    }

    private String getColumnDDLByField(ClassField field) {
        val sb = new StringBuilder();
        sb.append(field.getName())
                .append(" ")
                .append(ClassTypeUtil.pgFromMariaType(field.getType().name().toLowerCase()));
        if (field.getType().name().equalsIgnoreCase("varchar") && field.getSize() != null) {
            sb.append("(").append(field.getSize()).append(") ");
        } else {
            sb.append(" ");
        }
        if (! field.getNull()) {
            sb.append("NOT NULL");
        }
        return sb.toString();
    }

    private void appendPrimaryKeys(StringBuilder builder, String tableName, Collection<ClassField> pkList) {
        builder.append(DELIMITER)
                .append("constraint ")
                .append("pk_")
                .append(tableName.replace('.', '_'))
                .append(" primary key (")
                .append(pkList.stream().map(ClassField::getName).collect(Collectors.joining(DELIMITER)))
                .append(")");
    }

    private void appendShardingKeys(StringBuilder builder, Collection<ClassField> skList) {
        builder.append(" DISTRIBUTED BY (")
                .append(skList.stream().map(ClassField::getName).collect(Collectors.joining(DELIMITER)))
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
}
