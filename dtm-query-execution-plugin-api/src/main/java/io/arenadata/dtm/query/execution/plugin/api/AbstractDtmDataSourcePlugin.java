package io.arenadata.dtm.query.execution.plugin.api;

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckTableRequest;
import io.arenadata.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountRequest;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Request;
import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryRequest;
import io.arenadata.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprPluginRequest;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwPluginRequest;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.*;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckTableService;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.TruncateHistoryService;
import io.vertx.core.Future;

public abstract class AbstractDtmDataSourcePlugin implements DtmDataSourcePlugin {

    protected final DdlService<Void> ddlService;
    protected final LlrService<QueryResult> llrService;
    protected final MpprKafkaService mpprKafkaService;
    protected final MppwKafkaService mppwKafkaService;
    protected final QueryCostService<Integer> queryCostService;
    protected final StatusService statusService;
    protected final RollbackService<Void> rollbackService;
    protected final CheckTableService checkTableService;
    protected final CheckDataService checkDataService;
    protected final TruncateHistoryService truncateService;

    public AbstractDtmDataSourcePlugin(DdlService<Void> ddlService,
                                       LlrService<QueryResult> llrService,
                                       MpprKafkaService mpprKafkaService,
                                       MppwKafkaService mppwKafkaService,
                                       QueryCostService<Integer> queryCostService,
                                       StatusService statusService,
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
    public Future<Void> ddl(DdlRequestContext context) {
        return ddlService.execute(context);
    }

    @Override
    public Future<QueryResult> llr(LlrRequestContext context) {
        return llrService.execute(context);
    }

    @Override
    public Future<QueryResult> mppr(MpprPluginRequest request) {
        return mpprKafkaService.execute(request);
    }

    @Override
    public Future<QueryResult> mppw(MppwPluginRequest request) {
        return mppwKafkaService.execute(request);
    }

    @Override
    public Future<Integer> calcQueryCost(QueryCostRequestContext context) {
        return queryCostService.calc(context);
    }

    @Override
    public Future<StatusQueryResult> status(String topic) {
        return statusService.execute(topic);
    }

    @Override
    public Future<Void> rollback(RollbackRequestContext context) {
        return rollbackService.execute(context);
    }

    @Override
    public Future<Void> checkTable(CheckTableRequest request) {
        return checkTableService.check(request);
    }

    @Override
    public Future<Long> checkDataByCount(CheckDataByCountRequest request) {
        return checkDataService.checkDataByCount(request);
    }

    @Override
    public Future<Long> checkDataByHashInt32(CheckDataByHashInt32Request params) {
        return checkDataService.checkDataByHashInt32(params);
    }

    @Override
    public Future<Void> truncateHistory(TruncateHistoryRequest params) {
        return truncateService.truncateHistory(params);
    }
}
