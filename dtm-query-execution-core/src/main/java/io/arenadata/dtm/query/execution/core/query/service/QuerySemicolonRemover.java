package io.arenadata.dtm.query.execution.core.query.service;

import io.arenadata.dtm.common.reader.QueryRequest;

public interface QuerySemicolonRemover {
    QueryRequest remove(QueryRequest queryRequest);
}
