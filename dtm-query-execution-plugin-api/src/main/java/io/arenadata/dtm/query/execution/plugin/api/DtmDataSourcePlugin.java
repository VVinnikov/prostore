package io.arenadata.dtm.query.execution.plugin.api;

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckTableRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Request;
import io.arenadata.dtm.query.execution.plugin.api.dto.RollbackRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.QueryCostRequest;
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
     * @param request DDL context
     * @return void
     */
    Future<Void> ddl(DdlRequest request);

    /**
     * <p>execute Low Latency Reading</p>
     *
     * @param request LLR context
     * @return query result
     */
    Future<QueryResult> llr(LlrRequest request);

    /**
     * <p>prepare Low Latency Read query</p>
     *
     * @param request prepare llr request
     * @return void
     */
    Future<Void> prepareLlr(LlrRequest request);

    /**
     * <p>execute Massively Parallel Processing Reading</p>
     *
     * @param request MPPR context
     * @return query result
     */
    Future<QueryResult> mppr(MpprRequest request);

    /**
     * <p>execute Massively Parallel Processing Writing</p>
     *
     * @param request MPPW context
     * @return query result
     */
    Future<QueryResult> mppw(MppwRequest request);

    /**
     * <p>Get plugin status information</p>
     *
     * @param topic Topic
     * @return query status
     */
    Future<StatusQueryResult> status(String topic);

    /**
     * @param request Rollback request
     * @return void
     */
    Future<Void> rollback(RollbackRequest request);

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
