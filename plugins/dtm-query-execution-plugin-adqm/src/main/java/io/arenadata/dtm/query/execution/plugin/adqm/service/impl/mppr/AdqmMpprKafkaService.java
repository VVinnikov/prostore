package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppr;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.factory.MpprKafkaConnectorRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.service.MpprKafkaConnectorService;
import io.arenadata.dtm.query.execution.plugin.adqm.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.MpprRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.MpprKafkaService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("adqmMpprKafkaService")
public class AdqmMpprKafkaService implements MpprKafkaService<QueryResult> {
    private static final Logger LOG = LoggerFactory.getLogger(AdqmMpprKafkaService.class);

    private final QueryEnrichmentService adqmQueryEnrichmentService;
    private final MpprKafkaConnectorService mpprKafkaConnectorService;
    private final MpprKafkaConnectorRequestFactory requestFactory;

    public AdqmMpprKafkaService(@Qualifier("adqmQueryEnrichmentService") QueryEnrichmentService queryEnrichmentService,
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
                EnrichQueryRequest.generate(request.getQueryRequest(), request.getLogicalSchema()),
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