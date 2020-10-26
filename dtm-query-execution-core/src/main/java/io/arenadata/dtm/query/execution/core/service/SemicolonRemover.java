package io.arenadata.dtm.query.execution.core.service;

import io.arenadata.dtm.common.reader.QueryRequest;

public interface SemicolonRemover {
    QueryRequest remove(QueryRequest queryRequest);
}
