package ru.ibs.dtm.query.execution.plugin.api.request;

import io.vertx.core.json.JsonObject;
import ru.ibs.dtm.common.reader.QueryRequest;

public class QueryCostRequest extends DatamartRequest {
   private final JsonObject schema;

    public QueryCostRequest(QueryRequest queryRequest, JsonObject schema) {
        super(queryRequest);
        this.schema = schema;
    }

    public JsonObject getSchema() {
        return schema;
    }

}
