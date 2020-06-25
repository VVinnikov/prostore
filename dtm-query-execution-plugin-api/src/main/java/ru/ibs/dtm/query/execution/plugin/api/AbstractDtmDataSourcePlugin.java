package ru.ibs.dtm.query.execution.plugin.api;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.plugin.status.StatusQueryResult;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.*;
import ru.ibs.dtm.query.execution.plugin.api.status.KafkaStatusRequestContext;

public abstract class AbstractDtmDataSourcePlugin implements DtmDataSourcePlugin {

    protected final DdlService<Void> ddlService;
    protected final LlrService<QueryResult> llrService;
    protected final MpprKafkaService<QueryResult> mpprKafkaService;
    protected final MppwKafkaService<QueryResult> mppwKafkaService;
    protected final QueryCostService<Integer> queryCostService;
    protected final KafkaStatusService<StatusQueryResult> kafkaStatusService;

    public AbstractDtmDataSourcePlugin(DdlService<Void> ddlService,
                                       LlrService<QueryResult> llrService,
                                       MpprKafkaService<QueryResult> mpprKafkaService,
                                       MppwKafkaService<QueryResult> mppwKafkaService, QueryCostService<Integer> queryCostService,
                                       KafkaStatusService<StatusQueryResult> kafkaStatusService) {
        this.ddlService = ddlService;
        this.llrService = llrService;
        this.mpprKafkaService = mpprKafkaService;
        this.mppwKafkaService = mppwKafkaService;
        this.queryCostService = queryCostService;
        this.kafkaStatusService = kafkaStatusService;
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
    public void mpprKafka(MpprRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        mpprKafkaService.execute(context, asyncResultHandler);
    }

    @Override
    public void mppwKafka(MppwRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        mppwKafkaService.execute(context, asyncResultHandler);
    }

    @Override
    public void calcQueryCost(QueryCostRequestContext context,
                              Handler<AsyncResult<Integer>> asyncResultHandler) {
        queryCostService.calc(context, asyncResultHandler);
    }

    @Override
    public void kafkaStatus(KafkaStatusRequestContext context, Handler<AsyncResult<StatusQueryResult>> asyncResultHandler) {
        kafkaStatusService.execute(context, asyncResultHandler);
    }
}
