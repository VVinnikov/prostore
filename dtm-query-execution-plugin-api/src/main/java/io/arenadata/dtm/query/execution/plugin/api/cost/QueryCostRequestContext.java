package io.arenadata.dtm.query.execution.plugin.api.cost;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.QueryCostRequest;
import io.arenadata.dtm.common.model.SqlProcessingType;
import lombok.ToString;

import static io.arenadata.dtm.common.model.SqlProcessingType.COST;

@ToString
public class QueryCostRequestContext extends RequestContext<QueryCostRequest> {

    public QueryCostRequestContext(RequestMetrics metrics, QueryCostRequest request) {
        super(metrics, request);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return COST;
    }
}
