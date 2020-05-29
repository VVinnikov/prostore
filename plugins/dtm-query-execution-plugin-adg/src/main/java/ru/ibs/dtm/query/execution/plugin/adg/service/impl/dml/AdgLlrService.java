package ru.ibs.dtm.query.execution.plugin.adg.service.impl.dml;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adg.dto.EnrichQueryRequest;
import ru.ibs.dtm.query.execution.plugin.adg.model.QueryResultItem;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.adg.service.QueryExecutorService;
import ru.ibs.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.LlrRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.LlrService;

@Service("adgLlrService")
@Slf4j
public class AdgLlrService implements LlrService<QueryResult> {

	private final QueryEnrichmentService queryEnrichmentService;
	private final QueryExecutorService executorService;

	@Autowired
	public AdgLlrService(QueryEnrichmentService queryEnrichmentService,
						 QueryExecutorService executorService) {
		this.queryEnrichmentService = queryEnrichmentService;
		this.executorService = executorService;
	}

	@Override
	public void execute(LlrRequestContext context, Handler<AsyncResult<QueryResult>> handler) {
		LlrRequest request = context.getRequest();
		EnrichQueryRequest enrichQueryRequest = EnrichQueryRequest.generate(request.getQueryRequest(), request.getSchema());
		queryEnrichmentService.enrich(enrichQueryRequest, enrich -> {
			if (enrich.succeeded()) {
				executorService.execute(enrich.result(), exec -> {
					if (exec.succeeded()) {
						QueryResultItem queryResultItem = exec.result();
						JsonArray rowList = new JsonArray();
						try {
							queryResultItem.getDataSet().forEach(row -> {
								JsonObject jsonObject = new JsonObject();
								for (int i = 0; i < row.size(); i++) {
									jsonObject.put(
											queryResultItem.getMetadata().get(i).getName(),
											row.get(i));
								}
								rowList.add(jsonObject);
							});
						} catch (Exception e) {
							log.error("Ошибка разбора ответа на запрос {}", request, e);
							handler.handle(Future.failedFuture(e));
							return;
						}
						handler.handle(Future.succeededFuture(new QueryResult(request.getQueryRequest().getRequestId(), rowList)));
					} else {
						log.error("Ошибка выполнения запроса {}", request, exec.cause());
						handler.handle(Future.failedFuture(exec.cause()));
					}
				});
			} else {
				log.error("Ошибка при обогащении запроса {}", request);
				handler.handle(Future.failedFuture(enrich.cause()));
			}
		});
	}

}
