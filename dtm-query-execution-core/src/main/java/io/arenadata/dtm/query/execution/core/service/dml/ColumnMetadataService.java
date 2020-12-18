package io.arenadata.dtm.query.execution.core.service.dml;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.util.List;

public interface ColumnMetadataService {

    Future<List<ColumnMetadata>> getColumnMetadata(QueryParserRequest request);
}
