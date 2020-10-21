package ru.ibs.dtm.query.execution.core.service;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.plugin.status.StatusQueryResult;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.status.StatusRequestContext;

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
     * @param sourceType            Data source type
     * @param mppwRequestContext    MPPW context
     * @param resultHandler         async handler
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
     * @param sourceType            Data source type
     * @param statusRequestContext  Status request context
     * @param asyncResultHandler    async handler
     */
    void status(SourceType sourceType, StatusRequestContext statusRequestContext, Handler<AsyncResult<StatusQueryResult>> asyncResultHandler);

    /**
     * @param sourceType            Data source type
     * @param context               Rollback request context
     * @param asyncResultHandler    async handler
     */
    void rollback(SourceType sourceType, RollbackRequestContext context, Handler<AsyncResult<Void>> asyncResultHandler);
}
