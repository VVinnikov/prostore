package ru.ibs.dtm.query.execution.plugin.adg.service.impl.mppr;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.plugin.exload.QueryExloadParam;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adg.dto.EnrichQueryRequest;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtCartridgeClient;
import ru.ibs.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.MpprRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.MpprKafkaService;

import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@Service("adgMpprKafkaService")
public class AdgMpprKafkaService implements MpprKafkaService<QueryResult> {
	private final QueryEnrichmentService adbQueryEnrichmentService;
	private final TtCartridgeClient ttCartridgeClient;

	@Override
	public void execute(MpprRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
		MpprRequest request = context.getRequest();
		EnrichQueryRequest enrichQueryRequest = EnrichQueryRequest.generate(request.getQueryRequest(), request.getSchema());
		adbQueryEnrichmentService.enrich(enrichQueryRequest, sqlResult -> {
			if (sqlResult.succeeded()) {
				uploadData(request, asyncResultHandler, sqlResult.result());
			} else {
				log.error("Error while enriching request");
				asyncResultHandler.handle(Future.failedFuture(sqlResult.cause()));
			}
		});
	}

	private void uploadData(MpprRequest queryRequest,
							Handler<AsyncResult<QueryResult>> asyncResultHandler,
							String sql) {
		QueryExloadParam queryExloadParam = queryRequest.getQueryExloadParam();
		ttCartridgeClient.uploadData(sql, queryRequest.getTopic(), queryExloadParam.getChunkSize(), ar -> {
					UUID requestId = queryRequest.getQueryRequest().getRequestId();
					if (ar.succeeded()) {
						log.info("Uploading data from ADG was successful on request: {}", requestId);
						asyncResultHandler.handle(Future.succeededFuture(QueryResult.emptyResult()));
					} else {
						String errMsg = String.format("Error unloading data from ADG: %s on request %s",
								ar.cause().getMessage(),
								requestId);
						log.error(errMsg);
						asyncResultHandler.handle(Future.failedFuture(new RuntimeException(errMsg, ar.cause())));
					}
				}
		);
	}
}
