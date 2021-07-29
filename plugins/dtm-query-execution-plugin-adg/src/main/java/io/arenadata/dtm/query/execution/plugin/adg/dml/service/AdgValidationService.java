package io.arenadata.dtm.query.execution.plugin.adg.dml.service;

import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.query.execution.plugin.adg.base.exception.DtmTarantoolException;
import io.arenadata.dtm.query.execution.plugin.api.service.LlrValidationService;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Set;

@Service("adgValidationService")
public class AdgValidationService implements LlrValidationService {

    @Override
    public void validate(QueryParserResponse queryParserResponse) {
        Set<JoinRelType> joinTypes = new HashSet<>();
        queryParserResponse.getRelNode().project().accept(new RelHomogeneousShuttle(){
            @Override
            public RelNode visit(LogicalJoin join) {
                joinTypes.add(join.getJoinType());
                return super.visit(join);
            }
        });

        if (joinTypes.contains(JoinRelType.FULL)) {
            throw new DtmTarantoolException("Tarantool does not support FULL and FULL OUTER JOINs");
        }
    }
}
