package io.arenadata.dtm.query.execution.plugin.api;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
import io.arenadata.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountParams;
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
import org.springframework.plugin.core.Plugin;

import java.util.Set;

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
     *  @param context            DDL context
     * @param asyncResultHandler async handler
     */
    void ddl(DdlRequestContext context, AsyncHandler<Void> asyncResultHandler);

    /**
     * <p>execute Low Latency Reading</p>
     *
     * @param context            LLR context
     * @param asyncResultHandler async handler
     */
    void llr(LlrRequestContext context, AsyncHandler<QueryResult> asyncResultHandler);

    /**
     * <p>execute Massively Parallel Processing Reading</p>
     *
     * @param context            MPPR context
     * @param asyncResultHandler async handler
     */
    void mppr(MpprRequestContext context, AsyncHandler<QueryResult> asyncResultHandler);

    /**
     * <p>execute Massively Parallel Processing Writing</p>
     *
     * @param context            MPPW context
     * @param asyncResultHandler async handler
     */
    void mppw(MppwRequestContext context, AsyncHandler<QueryResult> asyncResultHandler);

    /**
     * <p>Calculate executing query cost</p>
     *
     * @param context            Query cost context
     * @param asyncResultHandler async handler
     */
    void calcQueryCost(QueryCostRequestContext context, AsyncHandler<Integer> asyncResultHandler);

    /**
     * <p>Get plugin status information</p>
     * @param context            Status request context
     * @param asyncResultHandler async handler
     */
    void status(StatusRequestContext context, AsyncHandler<StatusQueryResult> asyncResultHandler);

    /**
     *
     * @param context            Rollback request context
     * @param asyncResultHandler async handler
     */
    void rollback(RollbackRequestContext context, AsyncHandler<Void> asyncResultHandler);

    /**
     * <p>Get name set of active caches</p>
     *
     * @return set of caches names
     */
    Set<String> getActiveCaches();

    /**
     *
     * @param context CheckContext
     * @return error if check failed
     */
    Future<Void> checkTable(CheckContext context);

    /**
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
