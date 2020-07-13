package ru.ibs.dtm.query.calcite.core.service;

import io.vertx.core.Future;
import ru.ibs.dtm.common.reader.QueryRequest;

public interface DeltaQueryPreprocessor {
    Future<QueryRequest> process(QueryRequest request);
}
