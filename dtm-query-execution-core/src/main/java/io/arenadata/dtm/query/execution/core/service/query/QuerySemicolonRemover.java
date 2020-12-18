package io.arenadata.dtm.query.execution.core.service.query;

import io.arenadata.dtm.common.reader.QueryRequest;

public interface QuerySemicolonRemover {
    QueryRequest remove(QueryRequest queryRequest);
}
