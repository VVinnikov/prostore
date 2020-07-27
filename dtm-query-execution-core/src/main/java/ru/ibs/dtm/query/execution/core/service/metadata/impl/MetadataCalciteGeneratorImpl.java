package ru.ibs.dtm.query.execution.core.service.metadata.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.type.SqlTypeName;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.query.calcite.core.extension.ddl.SqlCreateTable;
import ru.ibs.dtm.query.calcite.core.extension.eddl.SqlNodeUtils;
import ru.ibs.dtm.query.execution.core.service.metadata.MetadataCalciteGenerator;
import ru.ibs.dtm.query.execution.core.utils.ColumnTypeUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class MetadataCalciteGeneratorImpl implements MetadataCalciteGenerator {

    @Override
    public ClassTable generateTableMetadata(SqlCreate sqlCreate) {
        final List<String> names = SqlNodeUtils.getTableNames(sqlCreate);
        final List<ClassField> fields = createTableFields(sqlCreate);
        return new ClassTable(getTableName(names), getSchema(names), fields);
    }

    private List<ClassField> createTableFields(SqlCreate sqlCreate) {
        final Map<String, ClassField> fieldMap = new HashMap<>();
        final List<ClassField> fields = new ArrayList<>();
        final SqlNodeList columnList = (SqlNodeList) sqlCreate.getOperandList().get(1);
        columnList.getList().forEach(col -> {
            if (col.getKind().equals(SqlKind.COLUMN_DECL)) {
                final ClassField field = createField((SqlColumnDeclaration) col);
                fieldMap.put(field.getName(), field);
                fields.add(field);
            } else if (col.getKind().equals(SqlKind.PRIMARY_KEY)) {
                initPrimaryKeyColumns((SqlKeyConstraint) col, fieldMap);
            } else {
                throw new RuntimeException("Тип атрибута " + col.getKind() + " не поддерживается!");
            }
        });
        initDistributedKeyColumns(sqlCreate, fieldMap);
        return fields;
    }

    private String getTableName(List<String> names) {
        return names.get(names.size() - 1);
    }

    private String getSchema(List<String> names) {
        return names.size() > 1 ? names.get(names.size() - 2) : null;
    }

    @NotNull
    private ClassField createField(SqlColumnDeclaration columnValue) {
        final SqlIdentifier column = getColumn(columnValue);
        final SqlDataTypeSpec columnType = getColumnType(columnValue);
        final ClassField field = new ClassField(column.getSimple(),
                ColumnTypeUtil.valueOf(SqlTypeName.get(columnType.getTypeName().getSimple().toUpperCase())),
                columnType.getNullable(), false);
        if (columnType.getTypeNameSpec() instanceof SqlBasicTypeNameSpec) {
            field.setSize(getPrecision(((SqlBasicTypeNameSpec) columnType.getTypeNameSpec())));
            field.setAccuracy(getScale(((SqlBasicTypeNameSpec) columnType.getTypeNameSpec())));
        }
        return field;
    }

    private void initPrimaryKeyColumns(SqlKeyConstraint col, Map<String, ClassField> fieldMap) {
        final List<SqlNode> pks = getPrimaryKeys(col);
        Integer pkOrder = 1;
        for (SqlNode pk : pks) {
            SqlIdentifier pkIdent = (SqlIdentifier) pk;
            ClassField keyfield = fieldMap.get(pkIdent.getSimple());
            keyfield.setPrimaryOrder(pkOrder);
            keyfield.setIsPrimary(true);
            pkOrder++;
        }
    }

    @NotNull
    private SqlIdentifier getColumn(SqlColumnDeclaration col) {
        return ((SqlIdentifier) col.getOperandList().get(0));
    }

    private SqlDataTypeSpec getColumnType(SqlColumnDeclaration col) {
        if (col.getOperandList().size() > 1) {
            return (SqlDataTypeSpec) col.getOperandList().get(1);
        } else {
            throw new RuntimeException("Ошибка определения типа столбца!");
        }
    }

    private List<SqlNode> getPrimaryKeys(SqlKeyConstraint col) {
        if (col.getOperandList().size() > 0) {
            return ((SqlNodeList) col.getOperandList().get(1)).getList();
        } else {
            throw new RuntimeException("Ошибка определения первичного ключа!");
        }
    }

    private Integer getPrecision(SqlBasicTypeNameSpec columnType) {
        return columnType.getPrecision() != -1 ? columnType.getPrecision() : null;
    }

    private Integer getScale(SqlBasicTypeNameSpec columnType) {
        return columnType.getScale() != -1 ? columnType.getScale() : null;
    }

    private void initDistributedKeyColumns(SqlCreate sqlCreate, Map<String, ClassField> fieldMap) {
        if (sqlCreate instanceof SqlCreateTable) {
            SqlCreateTable createTable = (SqlCreateTable) sqlCreate;
            SqlNodeList distributedBy = createTable.getDistributedBy().getDistributedBy();
            if (distributedBy != null) {
                List<SqlNode> distrColumnList = distributedBy.getList();
                if (distrColumnList != null) {
                    initDistributedOrderAttr(distrColumnList, fieldMap);
                }
            }
        }
    }

    private void initDistributedOrderAttr(List<SqlNode> distrColumnList, Map<String, ClassField> fieldMap) {
        Integer dkOrder = 1;
        for (SqlNode sqlNode : distrColumnList) {
            SqlIdentifier node = (SqlIdentifier) sqlNode;
            final ClassField field = fieldMap.get(node.getSimple());
            if (field == null) {
                throw new RuntimeException(String.format("Incorrect distributed key column name %s!", node.getSimple()));
            }
            field.setShardingOrder(dkOrder);
            dkOrder++;
        }
    }
}
