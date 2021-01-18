package io.arenadata.dtm.query.execution.plugin.api.request;

import io.arenadata.dtm.common.reader.QueryRequest;


public class DmlRequest extends DatamartRequest {

    public DmlRequest(final QueryRequest queryRequest) {
        super(queryRequest);
    }
}
