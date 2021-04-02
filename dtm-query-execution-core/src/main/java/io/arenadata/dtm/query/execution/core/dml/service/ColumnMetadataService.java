package io.arenadata.dtm.query.execution.core.dml.service;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.vertx.core.Future;
import org.apache.calcite.rel.RelRoot;

import java.util.List;

public interface ColumnMetadataService {

    Future<List<ColumnMetadata>> getColumnMetadata(QueryParserRequest request);

    Future<List<ColumnMetadata>> getColumnMetadata(RelRoot relNode);
}
