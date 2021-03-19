package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.query;

import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.node.SqlTreeNode;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.service.AdqmQueryJoinConditionsCheckService;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.calcite.sql.*;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class AdqmQueryJoinConditionsCheckServiceImpl implements AdqmQueryJoinConditionsCheckService {

    private static final String JOIN_REGEXP_PATH = ".*SELECT.JOIN";

    @Override
    public boolean isJoinConditionsCorrect(EnrichQueryRequest enrichQueryRequest) {
        Map<String, Map<String, Integer>> tableDistrKeyMap = new HashMap<>();
        enrichQueryRequest.getSchema().forEach(d -> {
            String schema = d.getMnemonic();
            tableDistrKeyMap.putAll(d.getEntities().stream().collect(Collectors.toMap(e -> getTableWithSchema(schema, e.getName()),
                    e -> e.getFields().stream()
                            .filter(f -> f.getShardingOrder() != null)
                            .collect(Collectors.toMap(EntityField::getName, EntityField::getShardingOrder))
            )));
        });

        SqlSelectTree selectTree = new SqlSelectTree(enrichQueryRequest.getQuery());
        List<SqlTreeNode> nodes = selectTree.findNodesByPathRegex(JOIN_REGEXP_PATH);
        for (SqlTreeNode node : nodes) {
            ConditionStat conditionStat = new ConditionStat();
            SqlJoin join = node.getNode();
            if (join.getCondition() instanceof SqlBasicCall) {
                SqlBasicCall joinCondition = (SqlBasicCall) join.getCondition();
                fillConditionStat(joinCondition, conditionStat);
                if (!isConditionsCorrect(tableDistrKeyMap, conditionStat)) {
                    return false;
                }
            } else {
                throw new DataSourceException("Unsupported condition node type");
            }
        }
        return true;
    }

    private static String getTableWithSchema(String schema, String name) {
        return schema + "." + name;
    }

    private boolean isConditionsCorrect(Map<String, Map<String, Integer>> tableDistrKeyMap, ConditionStat conditionStat) {
        if (conditionStat.getConditionList().size() == conditionStat.count) {
            int distrKeyCount = 0;
            String conditionTable = "";
            for (Pair<ConditionValue, ConditionValue> p : conditionStat.getConditionList().stream()
                    .distinct()
                    .collect(Collectors.toList())) {
                ConditionValue leftValue = p.getLeft();
                ConditionValue rightValue = p.getRight();
                Integer lNum = tableDistrKeyMap.get(leftValue.getTableWithSchema()).get(leftValue.getField());
                Integer rNum = tableDistrKeyMap.get(rightValue.getTableWithSchema()).get(rightValue.getField());
                conditionTable = leftValue.getTableWithSchema();
                if (lNum != null && lNum.equals(rNum)) {
                    distrKeyCount++;
                }
            }
            return tableDistrKeyMap.get(conditionTable).size() == distrKeyCount;
        }
        return false;
    }

    private void fillConditionStat(SqlBasicCall condition, ConditionStat conditionStat) {
        ConditionValue left = null;
        ConditionValue right = null;
        for (SqlNode sqlNode : condition.getOperandList()) {
            if (condition.getOperator().getKind() == SqlKind.EQUALS && sqlNode instanceof SqlIdentifier) {
                SqlIdentifier id = (SqlIdentifier) sqlNode;
                if (left == null) {
                    left = new ConditionValue(id.names.asList());
                } else {
                    right = new ConditionValue(id.names.asList());
                    conditionStat.getConditionList().add(Pair.of(left, right));
                    conditionStat.setCount(conditionStat.getCount() + 1);
                }
            } else if (condition.getOperator().getKind() == SqlKind.OR) {
                conditionStat.setCount(conditionStat.getCount() + 1);
                return;
            } else if (sqlNode instanceof SqlBasicCall) {
                fillConditionStat((SqlBasicCall) sqlNode, conditionStat);
            }
        }
    }

    @Data
    @NoArgsConstructor
    private static class ConditionStat {
        private int count = 0;
        private List<Pair<ConditionValue, ConditionValue>> conditionList = new ArrayList<>();
    }

    @Data
    @AllArgsConstructor
    private static class ConditionValue {
        private List<String> names;

        public String getTableWithSchema() {
            return AdqmQueryJoinConditionsCheckServiceImpl.getTableWithSchema(names.get(0), names.get(1));
        }

        public String getField() {
            return names.get(2);
        }
    }
}
