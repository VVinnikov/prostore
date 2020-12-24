package io.arenadata.dtm.query.execution.core.service.schema;

import io.arenadata.dtm.common.dto.schema.DatamartSchemaKey;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.util.Map;

public interface LogicalSchemaService {

    Future<Map<DatamartSchemaKey, Entity>> createSchemaFromQuery(QueryRequest request);

    Future<Map<DatamartSchemaKey, Entity>> createSchemaFromDeltaInformations(QueryRequest request);
}
