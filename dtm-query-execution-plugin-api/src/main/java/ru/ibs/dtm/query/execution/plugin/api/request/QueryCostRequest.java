package ru.ibs.dtm.query.execution.plugin.api.request;

import io.vertx.core.json.JsonObject;
import ru.ibs.dtm.common.cost.QueryCostAlgorithm;
import ru.ibs.dtm.common.reader.QueryRequest;

public class QueryCostRequest extends DatamartRequest {
    public static final QueryCostAlgorithm DEFAULT_ALGORITHM = QueryCostAlgorithm.DELAY_IN_SECONDS;
    private final QueryCostAlgorithm algorithm;
    private final JsonObject schema;

    public QueryCostRequest(QueryRequest queryRequest, JsonObject schema) {
        this(queryRequest, schema, DEFAULT_ALGORITHM);
    }

    public QueryCostRequest(QueryRequest queryRequest, JsonObject schema, QueryCostAlgorithm algorithm) {
        super(queryRequest);
        this.algorithm = algorithm;
        this.schema = schema;
    }

    public JsonObject getSchema() {
        return schema;
    }

    public QueryCostAlgorithm getAlgorithm() {
        return algorithm;
    }
}
