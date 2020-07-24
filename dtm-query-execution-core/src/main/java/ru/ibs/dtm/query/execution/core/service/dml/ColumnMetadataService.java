package ru.ibs.dtm.query.execution.core.service.dml;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import java.util.List;
import ru.ibs.dtm.common.reader.QuerySourceRequest;
import ru.ibs.dtm.query.execution.model.metadata.ColumnMetadata;

public interface ColumnMetadataService {
    void getColumnMetadata(QuerySourceRequest request, Handler<AsyncResult<List<ColumnMetadata>>> handler);
}
