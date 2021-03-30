package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.common.dml.SelectCategory;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.calcite.core.extension.dml.LimitableSqlOrderBy;
import io.arenadata.dtm.query.execution.core.service.dml.SelectCategoryQualifier;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import lombok.val;
import org.apache.calcite.sql.*;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

@Component
public class SelectCategoryQualifierImpl implements SelectCategoryQualifier {

    @Override
    public SelectCategory qualify(List<Datamart> schema, SqlNode query) {
        SqlSelect sqlSelect;
        if (query instanceof LimitableSqlOrderBy) {
            sqlSelect = (SqlSelect) ((LimitableSqlOrderBy) query).query;
        } else {
            sqlSelect = (SqlSelect) query;
        }
        if (isRelational(sqlSelect))
            return SelectCategory.RELATIONAL;
        if (isAnalytical(sqlSelect))
            return SelectCategory.ANALYTICAL;
        if (isDictionary(schema, sqlSelect))
            return SelectCategory.DICTIONARY;
        return SelectCategory.UNDEFINED;
    }

    private boolean isRelational(SqlSelect query) {
        if (query.getFrom().getKind().equals(SqlKind.JOIN) || checkSelectForSubquery(query)) {
            return true;
        } else if (query.hasWhere()) {
            return checkWhereForSubquery((SqlBasicCall) query.getWhere());
        } else {
            return false;
        }
    }

    private boolean checkSelectForSubquery(SqlSelect query) {
        return query.getSelectList().getList().stream()
                .anyMatch(sqlNode -> sqlNode instanceof SqlSelect);
    }

    private boolean checkWhereForSubquery(SqlBasicCall whereNode) {
        Queue<SqlNode> queue = new LinkedList<>();
        queue.add(whereNode);
        while (!queue.isEmpty()) {
            SqlNode childNode = queue.poll();
            if (childNode instanceof SqlSelect) {
                return true;
            } else if (childNode instanceof SqlBasicCall) {
                ((SqlBasicCall) childNode).getOperandList().forEach(queue::add);
            }
        }
        return false;
    }

    private boolean isAnalytical(SqlSelect query) {
        if (containsAggregateFunc(query) || query.getGroup() != null) {
            return true;
        } else {
            return false;
        }
    }

    private boolean containsAggregateFunc(SqlSelect select) {
        return select.getSelectList().getList().stream()
                .anyMatch(sqlNode -> sqlNode.getKind().equals(SqlKind.OTHER_FUNCTION));
    }

    private boolean isDictionary(List<Datamart> schema, SqlSelect query) {
        if (query.getWhere() != null) {
            List<String> primaryKeys = schema.get(0).getEntities().get(0).getFields().stream()
                    .filter(field -> field.getPrimaryOrder() != null)
                    .map(EntityField::getName)
                    .collect(Collectors.toList());
            return containsPrimaryKey(primaryKeys, query.getWhere());
        } else {
            return false;
        }
    }

    private boolean containsPrimaryKey(List<String> primaryKeys, SqlNode node) {
        if (node instanceof SqlIdentifier) {
            val conditionIdentifier = (SqlIdentifier) node;
            val conditionColumn = conditionIdentifier.names.get(conditionIdentifier.names.size() - 1);
            for (String primaryKey : primaryKeys) {
                if (conditionColumn.equalsIgnoreCase(primaryKey)) {
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
