package io.arenadata.dtm.query.execution.plugin.api;

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
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
     *
     * @param context            DDL context
     * @return void
     */
    Future<Void> ddl(DdlRequestContext context);

    /**
     * <p>execute Low Latency Reading</p>
     *
     * @param context LLR context
     * @return query result
     */
    Future<QueryResult> llr(LlrRequest request);

    /**
     * <p>execute Massively Parallel Processing Reading</p>
     *
     * @param context MPPR context
     * @return query result
     */
    Future<QueryResult> mppr(MpprRequestContext context);

    /**
     * <p>execute Massively Parallel Processing Writing</p>
     *
     * @param context MPPW context
     * @return query result
     */
    Future<QueryResult> mppw(MppwRequestContext context);

    /**
     * <p>Calculate executing query cost</p>
     *
     * @param context Query cost context
     * @return query cost
     */
    Future<Integer> calcQueryCost(QueryCostRequestContext context);

    /**
     * <p>Get plugin status information</p>
     *
     * @param context Status request context
     * @return query status
     */
    Future<StatusQueryResult> status(StatusRequestContext context);

    /**
     * @param context Rollback request context
     * @return void
     */
    Future<Void> rollback(RollbackRequestContext context);

    /**
     * <p>Get name set of active caches</p>
     *
     * @return set of caches names
     */
    Set<String> getActiveCaches();

    /**
     * @param request check table request
     * @return error if check failed
     */
    Future<Void> checkTable(CheckTableRequest request);

    /**
     * @param request CheckDataByCountParams
     * @return count of records
     */
    Future<Long> checkDataByCount(CheckDataByCountRequest request);

    /**
     * @param request CheckDataByHashInt32Params
     * @return checksum
     */
    Future<Long> checkDataByHashInt32(CheckDataByHashInt32Request request);

    /**
     * @param request truncate params
     * @return future object
     */
    Future<Void> truncateHistory(TruncateHistoryRequest request);
}
