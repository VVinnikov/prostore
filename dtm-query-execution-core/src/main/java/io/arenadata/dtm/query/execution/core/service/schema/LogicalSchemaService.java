package io.arenadata.dtm.query.execution.core.service.schema;

import io.arenadata.dtm.common.dto.schema.DatamartSchemaKey;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.Map;

public interface LogicalSchemaService {

    void createSchema(QueryRequest request, Handler<AsyncResult<Map<DatamartSchemaKey, Entity>>> resultHandler);
}
