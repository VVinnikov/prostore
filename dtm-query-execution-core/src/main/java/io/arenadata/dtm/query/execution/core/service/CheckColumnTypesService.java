package io.arenadata.dtm.query.execution.core.service;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.vertx.core.Future;

import java.util.List;

public interface CheckColumnTypesService {
    Future<Boolean> check(List<ColumnType> checkColumns, QueryParserRequest queryParseRequest);
}
