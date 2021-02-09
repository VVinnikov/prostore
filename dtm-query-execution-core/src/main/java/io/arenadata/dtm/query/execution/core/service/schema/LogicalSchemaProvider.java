package io.arenadata.dtm.query.execution.core.service.schema;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

public interface LogicalSchemaProvider {

    Future<List<Datamart>> getSchemaFromQuery(SqlNode query, String datamart);

    Future<List<Datamart>> getSchemaFromDeltaInformations(List<DeltaInformation> deltaInformations, String datamart);

    Future<List<Datamart>> updateSchema(QueryRequest request);
}
