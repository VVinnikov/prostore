package ru.ibs.dtm.query.execution.core.service.schema;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.dto.schema.DatamartSchemaKey;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.reader.QueryRequest;

import java.util.Map;

public interface LogicalSchemaService {

    void createSchema(QueryRequest request, Handler<AsyncResult<Map<DatamartSchemaKey, Entity>>> resultHandler);
}
