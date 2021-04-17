package io.arenadata.dtm.query.execution.core.base.service.metadata.impl;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityFieldUtils;
import io.arenadata.dtm.query.execution.core.base.service.metadata.DdlQueryGenerator;
import lombok.val;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class HsqlDdlQueryGenerator implements DdlQueryGenerator {

    public static final String DELIMITER = ", ";

    @Override
    public String generateCreateViewQuery(Entity entity) {
        val tableName = entity.getNameWithSchema();
        val sb = new StringBuilder()
                .append("CREATE VIEW ").append(tableName)
                .append(" AS ")
                .append(entity.getViewQuery());
        return sb.toString();
    }

    @Override
    public String generateCreateTableQuery(Entity entity) {
        val tableName = entity.getNameWithSchema();
        val sb = new StringBuilder()
                .append("CREATE TABLE ").append(tableName)
                .append(" (");
        appendClassTableFields(sb, entity.getFields());

        List<EntityField> pkList = EntityFieldUtils.getPrimaryKeyList(entity.getFields());
        if (pkList.size() > 0) {
            appendPrimaryKeys(sb, tableName, pkList);
        }
        sb.append(")");
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
                .append(getColumnType(field))
                .append(" ");
        if (!field.getNullable()) {
            sb.append("NOT NULL");
        }
        return sb.toString();
    }

    private String getColumnType(EntityField field) {
        switch (field.getType()) {
            case INT32:
            case INT:
                return "INTEGER";
            case UUID:
                return "VARCHAR(36)";
            case CHAR:
            case VARCHAR:
                return getFieldTypeWithSize(field.getSize(), field.getType());
            case TIME:
            case TIMESTAMP:
                return getFieldTypeWithSize(field.getAccuracy(), field.getType());
            default:
                return field.getType().toString();
        }
    }

    private String getFieldTypeWithSize(Integer fieldSize, ColumnType type) {
        val size = fieldSize == null ? "" : "(" + fieldSize + ")";
        return type.toString() + size;
    }

    private void appendPrimaryKeys(StringBuilder builder, String tableName, Collection<EntityField> pkList) {
        List<String> pkFields = pkList.stream().map(EntityField::getName).collect(Collectors.toList());
        builder.append(DELIMITER)
                .append("constraint ")
                .append("pk_")
                .append(tableName.replace('.', '_'))
                .append(" primary key (")
                .append(String.join(DELIMITER, pkFields))
                .append(")");
    }
}
