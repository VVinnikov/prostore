package io.arenadata.dtm.query.execution.core.base.service.schema;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.dto.schema.DatamartSchemaKey;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Map;

public interface LogicalSchemaService {

    Future<Map<DatamartSchemaKey, Entity>> createSchemaFromQuery(SqlNode sqlNode);

    Future<Map<DatamartSchemaKey, Entity>> createSchemaFromDeltaInformations(List<DeltaInformation> deltaInformations);
}
