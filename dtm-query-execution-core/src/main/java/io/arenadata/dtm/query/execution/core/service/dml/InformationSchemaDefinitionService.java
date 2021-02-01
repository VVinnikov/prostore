package io.arenadata.dtm.query.execution.core.service.dml;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

public interface InformationSchemaDefinitionService {

    boolean isInformationSchemaRequest(List<DeltaInformation> deltaInformations);

    Future<Void> checkAccessToSystemLogicalTables(SqlNode query);
}
