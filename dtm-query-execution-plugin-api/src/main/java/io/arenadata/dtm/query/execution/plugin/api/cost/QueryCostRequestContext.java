package io.arenadata.dtm.query.execution.plugin.api.cost;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.query.execution.plugin.api.CoreRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.QueryCostRequest;
import lombok.Getter;
import lombok.ToString;
import org.apache.calcite.sql.SqlNode;

import static io.arenadata.dtm.common.model.SqlProcessingType.COST;

@Getter
@ToString
public class QueryCostRequestContext extends CoreRequestContext<QueryCostRequest> {
    private final SqlNode query;
    public QueryCostRequestContext(RequestMetrics metrics, QueryCostRequest request, SqlNode query) {
        super(request, sqlNode, envName, metrics);
        this.query = query;
    }

    @Override
    public SqlProcessingType getProcessingType() {
        return COST;
    }
}
