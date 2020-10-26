package io.arenadata.dtm.query.calcite.core.service;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.vertx.core.Future;

public interface DeltaQueryPreprocessor {
    Future<QueryRequest> process(QueryRequest request);
}
