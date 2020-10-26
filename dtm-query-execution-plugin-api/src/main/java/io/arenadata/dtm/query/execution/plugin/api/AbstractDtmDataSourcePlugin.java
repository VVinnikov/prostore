package io.arenadata.dtm.query.execution.plugin.api;

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.*;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import io.arenadata.dtm.query.execution.plugin.api.status.StatusRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

public abstract class AbstractDtmDataSourcePlugin implements DtmDataSourcePlugin {

    protected final DdlService<Void> ddlService;
    protected final LlrService<QueryResult> llrService;
    protected final MpprKafkaService<QueryResult> mpprKafkaService;
    protected final MppwKafkaService<QueryResult> mppwKafkaService;
    protected final QueryCostService<Integer> queryCostService;
    protected final StatusService<StatusQueryResult> statusService;
    protected final RollbackService<Void> rollbackService;

    public AbstractDtmDataSourcePlugin(DdlService<Void> ddlService,
                                       LlrService<QueryResult> llrService,
                                       MpprKafkaService<QueryResult> mpprKafkaService,
                                       MppwKafkaService<QueryResult> mppwKafkaService,
                                       QueryCostService<Integer> queryCostService,
                                       StatusService<StatusQueryResult> statusService,
                                       RollbackService<Void> rollbackService) {
        this.ddlService = ddlService;
        this.llrService = llrService;
        this.mpprKafkaService = mpprKafkaService;
        this.mppwKafkaService = mppwKafkaService;
        this.queryCostService = queryCostService;
        this.statusService = statusService;
        this.rollbackService = rollbackService;
    }

    @Override
    public void ddl(DdlRequestContext context, Handler<AsyncResult<Void>> asyncResultHandler) {
        ddlService.execute(context, asyncResultHandler);
    }

    @Override
    public void llr(LlrRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        llrService.execute(context, asyncResultHandler);
    }

    @Override
    public void mppr(MpprRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        mpprKafkaService.execute(context, asyncResultHandler);
    }

    @Override
    public void mppw(MppwRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        mppwKafkaService.execute(context, asyncResultHandler);
    }

    @Override
    public void calcQueryCost(QueryCostRequestContext context,
                              Handler<AsyncResult<Integer>> asyncResultHandler) {
        queryCostService.calc(context, asyncResultHandler);
    }

    @Override
    public void status(StatusRequestContext context, Handler<AsyncResult<StatusQueryResult>> asyncResultHandler) {
        statusService.execute(context, asyncResultHandler);
    }

    @Override
    public void rollback(RollbackRequestContext context, Handler<AsyncResult<Void>> asyncResultHandler) {
        rollbackService.execute(context, asyncResultHandler);
    }
}
