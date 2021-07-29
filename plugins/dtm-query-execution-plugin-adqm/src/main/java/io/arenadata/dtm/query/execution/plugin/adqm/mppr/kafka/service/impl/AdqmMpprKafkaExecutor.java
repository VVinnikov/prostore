package io.arenadata.dtm.query.execution.plugin.adqm.mppr.kafka.service.impl;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.plugin.adqm.mppr.kafka.factory.MpprKafkaConnectorRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.mppr.AdqmMpprExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.mppr.kafka.service.MpprKafkaConnectorService;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.MpprKafkaRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("adqmMpprKafkaService")
@Slf4j
public class AdqmMpprKafkaExecutor implements AdqmMpprExecutor {

    private final QueryEnrichmentService adqmQueryEnrichmentService;
    private final MpprKafkaConnectorService mpprKafkaConnectorService;
    private final MpprKafkaConnectorRequestFactory requestFactory;
    private final QueryParserService queryParserService;

    @Autowired
    public AdqmMpprKafkaExecutor(@Qualifier("adqmQueryEnrichmentService") QueryEnrichmentService queryEnrichmentService,
                                 MpprKafkaConnectorService mpprKafkaConnectorService,
                                 MpprKafkaConnectorRequestFactory requestFactory,
                                 @Qualifier("adqmCalciteDMLQueryParserService") QueryParserService queryParserService) {
        this.adqmQueryEnrichmentService = queryEnrichmentService;
        this.mpprKafkaConnectorService = mpprKafkaConnectorService;
        this.requestFactory = requestFactory;
        this.queryParserService = queryParserService;
    }

    @Override
    public Future<QueryResult> execute(MpprRequest request) {
        val kafkaRequest = (MpprKafkaRequest) request;
        return queryParserService.parse(new QueryParserRequest(kafkaRequest.getDmlSubQuery(), kafkaRequest.getLogicalSchema()))
                .compose(parserResponse ->
                        adqmQueryEnrichmentService.enrich(EnrichQueryRequest.builder()
                                        .query(kafkaRequest.getDmlSubQuery())
                                        .deltaInformations(kafkaRequest.getDeltaInformations())
                                        .envName(kafkaRequest.getEnvName())
                                        .schema(kafkaRequest.getLogicalSchema())
                                        .build(),
                                parserResponse))
                .compose(enrichedQuery -> mpprKafkaConnectorService.call(
                        requestFactory.create(kafkaRequest, enrichedQuery)));
    }

    @Override
    public ExternalTableLocationType getType() {
        return ExternalTableLocationType.KAFKA;
    }
}
