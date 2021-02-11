package io.arenadata.dtm.query.execution.core.service.dml;

import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlNode;

import java.util.Set;

/**
 * Service defining acceptable source types for request
 */
public interface AcceptableSourceTypesDefinitionService {

    Future<Set<SourceType>> define(QuerySourceRequest request);
}
