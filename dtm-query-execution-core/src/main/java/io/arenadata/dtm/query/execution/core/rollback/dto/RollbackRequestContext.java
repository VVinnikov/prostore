package io.arenadata.dtm.query.execution.core.rollback.dto;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.core.base.dto.CoreRequestContext;
import org.apache.calcite.sql.SqlNode;

public class RollbackRequestContext extends CoreRequestContext<RollbackRequest, SqlNode> {

    public RollbackRequestContext(RequestMetrics metrics,
                                  String envName,
                                  RollbackRequest request,
                                  SqlNode sqlNode) {
        super(metrics, envName, request, sqlNode);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return SqlProcessingType.ROLLBACK;
    }
}
