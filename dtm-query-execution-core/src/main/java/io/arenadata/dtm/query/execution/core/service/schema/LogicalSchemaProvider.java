package io.arenadata.dtm.query.execution.core.service.schema;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.Future;

import java.util.List;

public interface LogicalSchemaProvider {

    Future<List<Datamart>> getSchemaFromQuery(QueryRequest request);

    Future<List<Datamart>> getSchemaFromDeltaInformations(QueryRequest request);

    Future<List<Datamart>> updateSchema(QueryRequest request);
}
