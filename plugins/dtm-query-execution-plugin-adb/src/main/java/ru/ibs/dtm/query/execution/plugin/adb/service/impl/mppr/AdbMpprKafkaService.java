package ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppr;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adb.dto.EnrichQueryRequest;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MpprKafkaConnectorRequestFactory;
import ru.ibs.dtm.query.execution.plugin.adb.service.MpprKafkaConnectorService;
import ru.ibs.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.MpprRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.MpprKafkaService;

@Service("adbMpprKafkaService")
public class AdbMpprKafkaService implements MpprKafkaService<QueryResult> {
	private static final Logger LOG = LoggerFactory.getLogger(AdbMpprKafkaService.class);

	private final QueryEnrichmentService adbQueryEnrichmentService;
	private final MpprKafkaConnectorService mpprKafkaConnectorService;
	private final MpprKafkaConnectorRequestFactory requestFactory;

	public AdbMpprKafkaService(QueryEnrichmentService queryEnrichmentService,
							   MpprKafkaConnectorService mpprKafkaConnectorService,
							   MpprKafkaConnectorRequestFactory requestFactory) {
		this.adbQueryEnrichmentService = queryEnrichmentService;
		this.mpprKafkaConnectorService = mpprKafkaConnectorService;
		this.requestFactory = requestFactory;
	}

	@Override
	public void execute(MpprRequestContext context, Handler<AsyncResult<QueryResult>> asyncHandler) {
		MpprRequest request = context.getRequest();
		adbQueryEnrichmentService.enrich(
				EnrichQueryRequest.generate(request.getQueryRequest(), request.getSchema()),
				sqlResult -> {
					if (sqlResult.succeeded()) {
						mpprKafkaConnectorService.call(
								requestFactory.create(request, sqlResult.result()),
								asyncHandler);
					} else {
						LOG.error("Error while enriching request");
						asyncHandler.handle(Future.failedFuture(sqlResult.cause()));
					}
				});
	}
}
