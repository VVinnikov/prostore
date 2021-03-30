package io.arenadata.dtm.query.execution.plugin.api.request;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.request.DatamartRequest;


public class ConfigRequest extends DatamartRequest {

    public ConfigRequest(QueryRequest queryRequest) {
        super(queryRequest);
    }
}
