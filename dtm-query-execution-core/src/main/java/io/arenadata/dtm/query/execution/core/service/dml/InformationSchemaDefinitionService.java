package io.arenadata.dtm.query.execution.core.service.dml;

import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.vertx.core.Future;

import java.util.Optional;

public interface InformationSchemaDefinitionService {

    Future<Optional<QuerySourceRequest>> tryGetInformationSchemaRequest(QuerySourceRequest request);
}
