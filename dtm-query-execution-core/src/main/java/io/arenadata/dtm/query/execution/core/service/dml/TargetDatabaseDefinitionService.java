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

    /**
     * Get target source type
     *
     * @param request request
     * @return future object
     */
    Future<QuerySourceRequest> getTargetSource(QuerySourceRequest request, SqlNode query);

    Future<Set<SourceType>> getAcceptableSourceTypes(QuerySourceRequest request);

    Future<SourceType> getSourceTypeWithLeastQueryCost(Set<SourceType> sourceTypes, QuerySourceRequest request);
}
