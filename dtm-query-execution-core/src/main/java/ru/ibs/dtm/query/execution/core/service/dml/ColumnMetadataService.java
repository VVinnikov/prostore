package ru.ibs.dtm.query.execution.core.service.dml;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.dto.QueryParserRequest;
import ru.ibs.dtm.query.execution.model.metadata.ColumnMetadata;

import java.util.List;

public interface ColumnMetadataService {
    void getColumnMetadata(QueryParserRequest request, Handler<AsyncResult<List<ColumnMetadata>>> handler);
}
