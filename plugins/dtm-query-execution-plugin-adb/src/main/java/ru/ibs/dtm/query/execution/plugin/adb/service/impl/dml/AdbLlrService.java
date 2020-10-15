package ru.ibs.dtm.query.execution.plugin.adb.service.impl.dml;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adb.dto.EnrichQueryRequest;
import ru.ibs.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.LlrRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.LlrService;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Service("adbLlrService")
@Slf4j
public class AdbLlrService implements LlrService<QueryResult> {

	private final QueryEnrichmentService adbQueryEnrichmentService;
	private final DatabaseExecutor adbDatabaseExecutor;

	public AdbLlrService(QueryEnrichmentService adbQueryEnrichmentService,
						 @Qualifier("adbQueryExecutor") DatabaseExecutor adbDatabaseExecutor) {
		this.adbQueryEnrichmentService = adbQueryEnrichmentService;
		this.adbDatabaseExecutor = adbDatabaseExecutor;
	}

	@Override
	public void execute(LlrRequestContext context, Handler<AsyncResult<QueryResult>> asyncHandler) {
		LlrRequest request = context.getRequest();
		EnrichQueryRequest enrichQueryRequest = EnrichQueryRequest.generate(request.getQueryRequest(), request.getSchema());
		adbQueryEnrichmentService.enrich(enrichQueryRequest, sqlResult -> {
			if (sqlResult.succeeded()) {
				adbDatabaseExecutor.execute(sqlResult.result(), request.getMetadata(), executeResult -> {
					if (executeResult.succeeded()) {
						QueryResult queryResult = QueryResult.emptyResult();
						queryResult.setRequestId(request.getQueryRequest().getRequestId());
						queryResult.setResult(executeResult.result());
						queryResult.setMetadata(request.getMetadata());
						asyncHandler.handle(Future.succeededFuture(queryResult));
					} else {
						asyncHandler.handle(Future.failedFuture(executeResult.cause()));
					}
				});
			} else {
				log.error("Error while enriching request");
				asyncHandler.handle(Future.failedFuture(sqlResult.cause()));
			}
		});
	}
}
