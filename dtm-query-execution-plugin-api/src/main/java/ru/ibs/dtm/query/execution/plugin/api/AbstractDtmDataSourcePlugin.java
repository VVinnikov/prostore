package ru.ibs.dtm.query.execution.plugin.api;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.api.dto.CalcQueryCostRequest;
import ru.ibs.dtm.query.execution.plugin.api.dto.DdlRequest;
import ru.ibs.dtm.query.execution.plugin.api.dto.LlrRequest;
import ru.ibs.dtm.query.execution.plugin.api.dto.MpprKafkaRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.DdlService;
import ru.ibs.dtm.query.execution.plugin.api.service.LlrService;
import ru.ibs.dtm.query.execution.plugin.api.service.MpprKafkaService;
import ru.ibs.dtm.query.execution.plugin.api.service.QueryCostService;

public abstract class AbstractDtmDataSourcePlugin implements DtmDataSourcePlugin {

  protected final DdlService ddlService;
  protected final LlrService llrService;
  protected final MpprKafkaService mpprKafkaService;
  protected final QueryCostService queryCostService;

  public AbstractDtmDataSourcePlugin(DdlService ddlService,
                                     LlrService llrService,
                                     MpprKafkaService mpprKafkaService,
                                     QueryCostService queryCostService) {
    this.ddlService = ddlService;
    this.llrService = llrService;
    this.mpprKafkaService = mpprKafkaService;
    this.queryCostService = queryCostService;
  }

  @Override
  public void ddl(DdlRequest request, Handler<AsyncResult<Void>> asyncResultHandler) {
    ddlService.execute(request, asyncResultHandler);
  }

  @Override
  public void llr(LlrRequest request, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
    llrService.executeQuery(request, asyncResultHandler);
  }

  @Override
  public void mpprKafka(MpprKafkaRequest request, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
    mpprKafkaService.execute(request, asyncResultHandler);
  }

  @Override
  public void calcQueryCost(CalcQueryCostRequest request,
                            Handler<AsyncResult<Integer>> asyncResultHandler) {
    queryCostService.calc(request, asyncResultHandler);
  }
}
