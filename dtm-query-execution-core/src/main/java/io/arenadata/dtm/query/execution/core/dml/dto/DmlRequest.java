package io.arenadata.dtm.query.execution.core.dml.dto;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.request.DatamartRequest;


public class DmlRequest extends DatamartRequest {
    public DmlRequest(final QueryRequest queryRequest) {
        super(queryRequest);
    }
}
