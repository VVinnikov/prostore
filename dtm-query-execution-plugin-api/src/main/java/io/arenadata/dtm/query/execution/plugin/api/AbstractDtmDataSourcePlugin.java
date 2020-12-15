package io.arenadata.dtm.query.execution.plugin.api;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
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
import io.arenadata.dtm.query.execution.plugin.api.service.*;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckTableService;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.arenadata.dtm.query.execution.plugin.api.status.StatusRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

public abstract class AbstractDtmDataSourcePlugin implements DtmDataSourcePlugin {

    protected final DdlService<Void> ddlService;
    protected final LlrService<QueryResult> llrService;
    protected final MpprKafkaService<QueryResult> mpprKafkaService;
    protected final MppwKafkaService<QueryResult> mppwKafkaService;
    protected final QueryCostService<Integer> queryCostService;
    protected final StatusService<StatusQueryResult> statusService;
    protected final RollbackService<Void> rollbackService;
    protected final CheckTableService checkTableService;
    protected final CheckDataService checkDataService;
    protected final TruncateHistoryService truncateService;

    public AbstractDtmDataSourcePlugin(DdlService<Void> ddlService,
                                       LlrService<QueryResult> llrService,
                                       MpprKafkaService<QueryResult> mpprKafkaService,
                                       MppwKafkaService<QueryResult> mppwKafkaService,
                                       QueryCostService<Integer> queryCostService,
                                       StatusService<StatusQueryResult> statusService,
                                       RollbackService<Void> rollbackService,
                                       CheckTableService checkTableService,
                                       CheckDataService checkDataService,
                                       TruncateHistoryService truncateService) {
        this.ddlService = ddlService;
        this.llrService = llrService;
        this.mpprKafkaService = mpprKafkaService;
        this.mppwKafkaService = mppwKafkaService;
        this.queryCostService = queryCostService;
        this.statusService = statusService;
        this.rollbackService = rollbackService;
        this.checkTableService = checkTableService;
        this.checkDataService = checkDataService;
        this.truncateService = truncateService;
    }

    @Override
    public void ddl(DdlRequestContext context, AsyncHandler<Void> asyncResultHandler) {
        ddlService.execute(context, asyncResultHandler);
    }

    @Override
    public void llr(LlrRequestContext context, AsyncHandler<QueryResult> asyncResultHandler) {
        llrService.execute(context, asyncResultHandler);
    }

    @Override
    public void mppr(MpprRequestContext context, AsyncHandler<QueryResult> asyncResultHandler) {
        mpprKafkaService.execute(context, asyncResultHandler);
    }

    @Override
    public void mppw(MppwRequestContext context, AsyncHandler<QueryResult> asyncResultHandler) {
        mppwKafkaService.execute(context, asyncResultHandler);
    }

    @Override
    public void calcQueryCost(QueryCostRequestContext context,
                              AsyncHandler<Integer> asyncResultHandler) {
        queryCostService.calc(context, asyncResultHandler);
    }

    @Override
    public void status(StatusRequestContext context, AsyncHandler<StatusQueryResult> asyncResultHandler) {
        statusService.execute(context, asyncResultHandler);
    }

    @Override
    public void rollback(RollbackRequestContext context, AsyncHandler<Void> asyncResultHandler) {
        rollbackService.execute(context, asyncResultHandler);
    }

    @Override
    public Future<Void> checkTable(CheckContext context) {
        return checkTableService.check(context);
    }

    @Override
    public Future<Long> checkDataByCount(CheckDataByCountParams params) {
        return checkDataService.checkDataByCount(params);
    }

    @Override
    public Future<Long> checkDataByHashInt32(CheckDataByHashInt32Params params) {
        return checkDataService.checkDataByHashInt32(params);
    }

    @Override
    public Future<Void> truncateHistory(TruncateHistoryParams params) {
        return truncateService.truncateHistory(params);
    }
}
