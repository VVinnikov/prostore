package io.arenadata.dtm.query.execution.core.service.dml;

import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.vertx.core.Future;

public interface InformationSchemaDefinitionService {

    Future<QuerySourceRequest> tryGetInformationSchemaRequest(QuerySourceRequest request);
}
