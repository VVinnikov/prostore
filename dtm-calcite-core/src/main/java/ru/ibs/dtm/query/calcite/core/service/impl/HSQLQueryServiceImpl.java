package ru.ibs.dtm.query.calcite.core.service.impl;

import lombok.val;
import ru.ibs.dtm.common.model.ddl.*;
import ru.ibs.dtm.query.calcite.core.service.HSQLQueryService;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class HSQLQueryServiceImpl implements HSQLQueryService {

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

    private String getColumnType(EntityField field){
        return ColumnType.VARCHAR.equals(field.getType()) ? "VARCHAR" + getVarcharSize(field) : field.getType().toString();
    }

    private static String getVarcharSize(EntityField field) {
        return field.getSize() == null ? "" : "(" + field.getSize() + ")";
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
