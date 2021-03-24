package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.query.execution.core.service.dml.SqlParametersTypeExtractor;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.type.SqlTypeName;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class SqlParametersTypeExtractorImpl implements SqlParametersTypeExtractor {

    @Override
    public List<SqlTypeName> extract(RelNode relNode) {
        Set<RexDynamicParam> dynamicParams = new HashSet<>();
        getParameterTypes(relNode, dynamicParams);
        return dynamicParams.stream()
                .sorted(Comparator.comparing(RexDynamicParam::getIndex))
                .map(rn -> rn.getType().getSqlTypeName())
                .collect(Collectors.toList());
    }

    private void getParameterTypes(RelNode relNode, Set<RexDynamicParam> dynamicParams) {
        relNode.accept(new RelHomogeneousShuttle() {
            @Override
            protected RelNode visitChild(RelNode parent, int i, RelNode child) {
                getParameterTypes(child, dynamicParams);
                return super.visitChild(parent, i, child);
            }
        });
        relNode.accept(new RexShuttle() {
            @Override
            public RexNode visitSubQuery(RexSubQuery subQuery) {
                getParameterTypes(subQuery.rel, dynamicParams);
                return super.visitSubQuery(subQuery);
            }

            @Override
            public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
                dynamicParams.add(dynamicParam);
                return super.visitDynamicParam(dynamicParam);
            }
        });
    }
}
