package io.arenadata.dtm.query.execution.core.service.schema;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.List;

public interface LogicalSchemaProvider {

    void getSchema(QueryRequest request, Handler<AsyncResult<List<Datamart>>> resultHandler);

    void updateSchema(QueryRequest request, Handler<AsyncResult<List<Datamart>>> resultHandler);
}
