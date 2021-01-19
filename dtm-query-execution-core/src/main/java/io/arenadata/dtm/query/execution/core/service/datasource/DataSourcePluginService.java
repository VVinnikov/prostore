package io.arenadata.dtm.query.execution.core.service.datasource;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.api.DtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckTableRequest;
import io.arenadata.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Request;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.status.StatusRequestContext;
import io.vertx.core.Future;

import java.util.Set;

/**
 * Service for interaction of the core with data source plugins
 */
public interface DataSourcePluginService {

    /**
     * <p>Get data source type</p>
     *
     * @return Data source type
     */
    Set<SourceType> getSourceTypes();

    /**
     * <p>execute DDL operation</p>
     *
     * @param sourceType Data source type
     * @param context    DDL context
     * @return future object
     */
    Future<Void> ddl(SourceType sourceType, DdlRequestContext context);

    /**
     * <p>execute Low Latency Reading request</p>
     *
     * @param sourceType Data source type
     * @param metrics    metrics
     * @param llrRequest    llr request
     * @return future object
     */
    Future<QueryResult> llr(SourceType sourceType, RequestMetrics metrics, LlrRequest llrRequest);

    /**
     * <p>execute Massively Parallel Processing Reading</p>
     *
     * @param sourceType Data source type
     * @param context    MPPR context
     * @return future object
     */
    Future<QueryResult> mppr(SourceType sourceType, MpprRequestContext context);

    /**
     * <p>execute Massively Parallel Processing Writing</p>
     *
     * @param sourceType         Data source type
     * @param mppwRequestContext MPPW context
     * @return future object
     */
    Future<QueryResult> mppw(SourceType sourceType, MppwRequestContext mppwRequestContext);

    /**
     * <p>Calculate executing query cost</p>
     *
     * @param sourceType Data source type
     * @param context    Query cost context
     * @return future object
     */
    Future<Integer> calcQueryCost(SourceType sourceType, QueryCostRequestContext context);

    /**
     * <p>Get plugin status information</p>
     *
     * @param sourceType           Data source type
     * @param statusRequestContext Status request context
     * @return future object
     */
    Future<StatusQueryResult> status(SourceType sourceType, StatusRequestContext statusRequestContext);

    /**
     * @param sourceType Data source type
     * @param context    Rollback request context
     * @return future object
     */
    Future<Void> rollback(SourceType sourceType, RollbackRequestContext context);

    /**
     * Get plugin by source type
     *
     * @param sourceType Data source type
     * @return plugin
     */
    DtmDataSourcePlugin getPlugin(SourceType sourceType);

    /**
     * <p>Get name set of active caches</p>
     *
     * @return set of caches names
     */
    Set<String> getActiveCaches();

    /**
     * @param sourceType   SourceType
     * @param checkTableRequest
     * @return failed future with errors if check failed
     */
    Future<Void> checkTable(SourceType sourceType, RequestMetrics metrics, CheckTableRequest checkTableRequest);

    /**
     * @param request CheckDataByCountParams
     * @return count of records
     */
    Future<Long> checkDataByCount(SourceType sourceType, RequestMetrics metrics, CheckDataByCountRequest request);

    /**
     * @param request CheckDataByHashInt32Params
     * @return checksum
     */
    Future<Long> checkDataByHashInt32(SourceType sourceType, RequestMetrics metrics, CheckDataByHashInt32Request request);

    /**
     * @param params TruncateHistoryParams
     * @return void
     */
    Future<Void> truncateHistory(SourceType sourceType, RequestMetrics metrics, TruncateHistoryRequest params);
}
