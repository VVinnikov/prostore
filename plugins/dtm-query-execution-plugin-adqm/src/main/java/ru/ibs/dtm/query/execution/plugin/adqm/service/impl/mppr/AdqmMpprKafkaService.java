package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.mppr;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adqm.dto.EnrichQueryRequest;
import ru.ibs.dtm.query.execution.plugin.adqm.factory.MpprKafkaConnectorRequestFactory;
import ru.ibs.dtm.query.execution.plugin.adqm.service.MpprKafkaConnectorService;
import ru.ibs.dtm.query.execution.plugin.adqm.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.MpprRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.MpprKafkaService;

@Service("adqmMpprKafkaService")
public class AdqmMpprKafkaService implements MpprKafkaService<QueryResult> {
    private static final Logger LOG = LoggerFactory.getLogger(AdqmMpprKafkaService.class);

    private final QueryEnrichmentService adqmQueryEnrichmentService;
    private final MpprKafkaConnectorService mpprKafkaConnectorService;
    private final MpprKafkaConnectorRequestFactory requestFactory;

    public AdqmMpprKafkaService(QueryEnrichmentService queryEnrichmentService,
                                MpprKafkaConnectorService mpprKafkaConnectorService,
                                MpprKafkaConnectorRequestFactory requestFactory) {
        this.adqmQueryEnrichmentService = queryEnrichmentService;
        this.mpprKafkaConnectorService = mpprKafkaConnectorService;
        this.requestFactory = requestFactory;
    }

    @Override
    public void execute(MpprRequestContext context, Handler<AsyncResult<QueryResult>> asyncHandler) {
        MpprRequest request = context.getRequest();
        adqmQueryEnrichmentService.enrich(
                EnrichQueryRequest.generate(request.getQueryRequest(), request.getSchema()),
                sqlResult -> {
                    if (sqlResult.succeeded()) {
                        mpprKafkaConnectorService.call(
                                requestFactory.create(request, sqlResult.result()),
                                asyncHandler);
                    } else {
                        LOG.error("Ошибка при обогащении запроса");
                        asyncHandler.handle(Future.failedFuture(sqlResult.cause()));
                    }
                });
    }
}
