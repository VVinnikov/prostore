package io.arenadata.dtm.query.execution.core.service.dml;

import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlNode;

import java.util.Set;

/**
 * Service for target database definition
 */
public interface TargetDatabaseDefinitionService {

    Future<Set<SourceType>> getAcceptableSourceTypes(QuerySourceRequest request);

    Future<SourceType> getSourceTypeWithLeastQueryCost(Set<SourceType> sourceTypes, QuerySourceRequest request);
}
