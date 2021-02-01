package io.arenadata.dtm.query.execution.plugin.api.request;

import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.dto.PluginRequest;

import java.util.List;
import java.util.UUID;

public class QueryCostRequest extends PluginRequest {
    private final List<Datamart> schema;

    public QueryCostRequest(UUID requestId,
                            String envName,
                            String datamart,
                            List<Datamart> schema) {
        super(requestId, envName, datamart);
        this.schema = schema;
    }

    public List<Datamart> getSchema() {
        return schema;
    }

}
