package io.arenadata.dtm.query.execution.plugin.api.request;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;

import java.util.List;

public class QueryCostRequest extends DatamartRequest {
    private final List<Datamart> schema;

    public QueryCostRequest(QueryRequest queryRequest, List<Datamart> schema) {
        super(queryRequest);
        this.schema = schema;
    }

    public List<Datamart> getSchema() {
        return schema;
    }

}
