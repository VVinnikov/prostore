package ru.ibs.dtm.query.execution.plugin.api;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.springframework.plugin.core.Plugin;
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

/**
 * Data source plugin interface
 */
public interface DtmDataSourcePlugin extends Plugin<SourceType> {

    /**
     * <p>Data source type support</p>
     *
     * @param sourceType data source type
     * @return is support
     */
    default boolean supports(SourceType sourceType) {
        return getSourceType() == sourceType;
    }

    /**
     * <p>Get data source type</p>
     *
     * @return data source type
     */
    SourceType getSourceType();

    /**
     * <p>execute DDL operation</p>
     *
     * @param context            DDL context
     * @param asyncResultHandler async handler
     */
    void ddl(DdlRequestContext context, Handler<AsyncResult<Void>> asyncResultHandler);

    /**
     * <p>execute Low Latency Reading</p>
     *
     * @param context            LLR context
     * @param asyncResultHandler async handler
     */
    void llr(LlrRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler);

    /**
     * <p>execute Massively Parallel Processing Reading</p>
     *
     * @param context            MPPR context
     * @param asyncResultHandler async handler
     */
    void mppr(MpprRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler);

    /**
     * <p>execute Massively Parallel Processing Writing</p>
     *
     * @param context            MPPW context
     * @param asyncResultHandler async handler
     */
    void mppw(MppwRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler);

    /**
     * <p>Calculate executing query cost</p>
     *
     * @param context            Query cost context
     * @param asyncResultHandler async handler
     */
    void calcQueryCost(QueryCostRequestContext context, Handler<AsyncResult<Integer>> asyncResultHandler);

    /**
     * <p>Get plugin status information</p>
     * @param context            Status request context
     * @param asyncResultHandler async handler
     */
    void status(StatusRequestContext context, Handler<AsyncResult<StatusQueryResult>> asyncResultHandler);

    /**
     *
     * @param context            Rollback request context
     * @param asyncResultHandler async handler
     */
    void rollback(RollbackRequestContext context, Handler<AsyncResult<Void>> asyncResultHandler);
}
