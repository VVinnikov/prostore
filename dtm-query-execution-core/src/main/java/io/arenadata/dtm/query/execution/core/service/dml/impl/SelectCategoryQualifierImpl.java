package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.common.dml.SelectCategory;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.core.service.dml.SelectCategoryQualifier;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import lombok.val;
import org.apache.calcite.sql.*;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;

@Component
public class SelectCategoryQualifierImpl implements SelectCategoryQualifier {

    @Override
    public SelectCategory qualify(List<Datamart> schema, SqlNode query) {
        val sqlSelect = (SqlSelect) query;
        if (isRelational(sqlSelect))
            return SelectCategory.RELATIONAL;
        if (isAnalytical(sqlSelect))
            return SelectCategory.ANALYTICAL;
        if (isDictionary(schema, sqlSelect))
            return SelectCategory.DICTIONARY;
        return SelectCategory.UNDEFINED;
    }

    private boolean isRelational(SqlSelect query) {
        if (query.getFrom().getKind().equals(SqlKind.JOIN) || query.getSelectList().getList().stream()
                .anyMatch(sqlNode -> sqlNode instanceof SqlSelect)) return true;
        if (query.hasWhere()) {
            return checkWhereForSubquery((SqlBasicCall) query.getWhere());
        }
        return false;
    }

    private boolean checkWhereForSubquery(SqlBasicCall whereNode) {
        Stack<SqlNode> stack = new Stack<>();
        stack.push(whereNode);
        while (!stack.isEmpty()) {
            SqlNode childNode = stack.pop();
            if (childNode instanceof SqlSelect) {
                return true;
            } else if (childNode instanceof SqlBasicCall) {
                ((SqlBasicCall) childNode).getOperandList().stream().forEach(stack::push);
            }
        }
        return false;
    }

    private boolean isAnalytical(SqlSelect query) {
        if (query.getSelectList().getList().stream().anyMatch(sqlNode -> sqlNode.getKind().equals(SqlKind.OTHER_FUNCTION))
                || query.getGroup() != null) return true;
        return false;
    }

    private boolean isDictionary(List<Datamart> schema, SqlSelect query) {
        List<String> primaryKeys = schema.get(0).getEntities().get(0).getFields().stream()
                .filter(field -> field.getPrimaryOrder() != null)
                .map(EntityField::getName)
                .collect(Collectors.toList());
        return containsPrimaryKey(primaryKeys, query.getWhere());
    }

    private boolean containsPrimaryKey(List<String> primaryKeys, SqlNode node) {
        if (node instanceof SqlIdentifier) {
            val conditionColumn = (SqlIdentifier) node;
            for (String primaryKey : primaryKeys) {
                if (conditionColumn.names.get(conditionColumn.names.size() - 1).equalsIgnoreCase(primaryKey)) {
                    return true;
                }
            }
        } else if (node.getKind().equals(SqlKind.OR)) {
            return containsPrimaryKeyInOr(primaryKeys, (SqlBasicCall) node);
        } else if (node instanceof SqlBasicCall) {
            return containsPrimaryKeyInOther(primaryKeys, (SqlBasicCall) node);
        }
        return false;
    }

    private boolean containsPrimaryKeyInOr(List<String> primaryKeys, SqlBasicCall orNode) {
        SqlNode leftNode = orNode.getOperandList().get(0);
        SqlNode rightNode = orNode.getOperandList().get(1);
        return containsPrimaryKey(primaryKeys, leftNode) && containsPrimaryKey(primaryKeys, rightNode);
    }

    private boolean containsPrimaryKeyInOther(List<String> primaryKeys, SqlBasicCall node) {
        for (SqlNode operand : node.getOperandList()) {
            if (containsPrimaryKey(primaryKeys, operand)) {
                return true;
            }
        }
        return false;
    }
}
