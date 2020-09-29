package ru.ibs.dtm.query.execution.core.service.schema;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;

import java.util.List;

public interface LogicalSchemaProvider {

    void getSchema(QueryRequest request, Handler<AsyncResult<List<Datamart>>> resultHandler);

    void updateSchema(QueryRequest request, Handler<AsyncResult<List<Datamart>>> resultHandler);
}
