package io.arenadata.dtm.query.execution.core.service.query;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.vertx.core.Future;

import java.util.List;

public interface CheckColumnTypesService {
    Future<Boolean> check(List<EntityField> destinationColumns, QueryParserRequest queryParseRequest);
}
