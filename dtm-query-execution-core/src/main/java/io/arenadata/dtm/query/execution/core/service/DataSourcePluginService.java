package io.arenadata.dtm.query.execution.core.service;

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountParams;
import io.arenadata.dtm.query.execution.plugin.api.DtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
import io.arenadata.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Params;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryParams;
import io.arenadata.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.status.StatusRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

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
     * @param sourceType         Data source type
     * @param context            DDL context
     * @param asyncResultHandler async handler
     */
    void ddl(SourceType sourceType,
             DdlRequestContext context,
             Handler<AsyncResult<Void>> asyncResultHandler);

    /**
     * <p>execute Low Latency Reading request</p>
     *
     * @param sourceType         Data source type
     * @param context            LLR context
     * @param asyncResultHandler async handler
     */
    void llr(SourceType sourceType,
             LlrRequestContext context,
             Handler<AsyncResult<QueryResult>> asyncResultHandler);

    /**
     * <p>execute Massively Parallel Processing Reading</p>
     *
     * @param sourceType         Data source type
     * @param context            MPPR context
     * @param asyncResultHandler async handler
     */
    void mppr(SourceType sourceType,
              MpprRequestContext context,
              Handler<AsyncResult<QueryResult>> asyncResultHandler);

    /**
     * <p>execute Massively Parallel Processing Writing</p>
     *
     * @param sourceType         Data source type
     * @param mppwRequestContext MPPW context
     * @param resultHandler      async handler
     */
    void mppw(SourceType sourceType, MppwRequestContext mppwRequestContext,
              Handler<AsyncResult<QueryResult>> resultHandler);

    /**
     * <p>Calculate executing query cost</p>
     *
     * @param sourceType         Data source type
     * @param context            Query cost context
     * @param asyncResultHandler async handler
     */
    void calcQueryCost(SourceType sourceType,
                       QueryCostRequestContext context,
                       Handler<AsyncResult<Integer>> asyncResultHandler);

    /**
     * <p>Get plugin status information</p>
     *
     * @param sourceType           Data source type
     * @param statusRequestContext Status request context
     * @param asyncResultHandler   async handler
     */
    void status(SourceType sourceType, StatusRequestContext statusRequestContext, Handler<AsyncResult<StatusQueryResult>> asyncResultHandler);

    /**
     * @param sourceType         Data source type
     * @param context            Rollback request context
     * @param asyncResultHandler async handler
     */
    void rollback(SourceType sourceType, RollbackRequestContext context, Handler<AsyncResult<Void>> asyncResultHandler);

    /**
     * Get plugin by source type
     *
     * @param sourceType Data source type
     * @return plugin
     */
    DtmDataSourcePlugin getPlugin(SourceType sourceType);

    /**
     *
     * @param sourceType SourceType
     * @param context CheckContext
     * @return failed future with errors if check failed
     */
    Future<Void> checkTable(SourceType sourceType, CheckContext context);

    /**
     *
     *
     * @param params CheckDataByCountParams
     * @return count of records
     */
    Future<Long> checkDataByCount(CheckDataByCountParams params);

    /**
     *
     * @param params CheckDataByHashInt32Params
     * @return checksum
     */
    Future<Long> checkDataByHashInt32(CheckDataByHashInt32Params params);

    Future<Void> truncateHistory(TruncateHistoryParams params);
}
