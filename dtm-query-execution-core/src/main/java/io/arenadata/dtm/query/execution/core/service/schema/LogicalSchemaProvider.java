package io.arenadata.dtm.query.execution.core.service.schema;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.util.List;

public interface LogicalSchemaProvider {

    Future<List<Datamart>> getSchema(QueryRequest requestr);

    Future<List<Datamart>> updateSchema(QueryRequest request);
}
