package io.arenadata.dtm.query.execution.plugin.api.cost;

import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.QueryCostRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.SqlProcessingType;
import lombok.ToString;

import static io.arenadata.dtm.query.execution.plugin.api.service.SqlProcessingType.COST;

@ToString
public class QueryCostRequestContext extends RequestContext<QueryCostRequest> {

    public QueryCostRequestContext(QueryCostRequest request) {
        super(request);
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return COST;
    }
}
