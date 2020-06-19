package ru.ibs.dtm.query.execution.plugin.api.cost;

import lombok.ToString;
import ru.ibs.dtm.query.execution.plugin.api.RequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.QueryCostRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType;

import static ru.ibs.dtm.query.execution.plugin.api.service.SqlProcessingType.COST;

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
