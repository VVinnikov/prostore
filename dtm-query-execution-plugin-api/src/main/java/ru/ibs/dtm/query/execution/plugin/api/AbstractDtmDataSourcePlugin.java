package ru.ibs.dtm.query.execution.plugin.api;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.api.cost.QueryCostRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.LlrService;
import ru.ibs.dtm.query.execution.plugin.api.service.MpprKafkaService;
import ru.ibs.dtm.query.execution.plugin.api.service.QueryCostService;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;

public abstract class AbstractDtmDataSourcePlugin implements DtmDataSourcePlugin {

	protected final DdlService<Void> ddlService;
	protected final LlrService<QueryResult> llrService;
	protected final MpprKafkaService<QueryResult> mpprKafkaService;
	protected final QueryCostService<Integer> queryCostService;

	public AbstractDtmDataSourcePlugin(DdlService<Void> ddlService,
									   LlrService<QueryResult> llrService,
									   MpprKafkaService<QueryResult> mpprKafkaService,
									   QueryCostService<Integer> queryCostService) {
		this.ddlService = ddlService;
		this.llrService = llrService;
		this.mpprKafkaService = mpprKafkaService;
		this.queryCostService = queryCostService;
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
	public void calcQueryCost(QueryCostRequestContext context,
							  Handler<AsyncResult<Integer>> asyncResultHandler) {
		queryCostService.calc(context, asyncResultHandler);
	}
}
