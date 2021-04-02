package io.arenadata.dtm.query.execution.plugin.adg.mppr.kafka.service;

import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adg.enrichment.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.request.AdgUploadDataKafkaRequest;
import io.arenadata.dtm.query.execution.plugin.adg.base.service.client.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.adg.mppr.AdgMpprExecutor;
import io.arenadata.dtm.query.execution.plugin.adg.enrichment.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.api.exception.MpprDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.DownloadExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.MpprKafkaRequest;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service("adgMpprKafkaService")
public class AdgMpprKafkaService implements AdgMpprExecutor {
    private final QueryEnrichmentService adbQueryEnrichmentService;
    private final AdgCartridgeClient adgCartridgeClient;

    @Override
    public Future<QueryResult> execute(MpprRequest request) {
        return Future.future(promise -> {
            EnrichQueryRequest enrichQueryRequest = EnrichQueryRequest.builder()
                    .query(((MpprKafkaRequest) request).getDmlSubQuery())
                    .deltaInformations(request.getDeltaInformations())
                    .envName(request.getEnvName())
                    .schema(request.getLogicalSchema())
                    .build();
            adbQueryEnrichmentService.enrich(enrichQueryRequest)
                    .compose(enrichQuery -> uploadData((MpprKafkaRequest) request, enrichQuery))
                    .onComplete(promise);
        });
    }

    @Override
    public ExternalTableLocationType getType() {
        return ExternalTableLocationType.KAFKA;
    }

    private Future<QueryResult> uploadData(MpprKafkaRequest kafkaRequest, String sql) {
        return Future.future(promise -> {
            val downloadMetadata =
                    (DownloadExternalEntityMetadata) kafkaRequest.getDownloadMetadata();
            val request = new AdgUploadDataKafkaRequest(
                    sql,
                    kafkaRequest.getTopic(),
                    downloadMetadata.getChunkSize(),
                    new JsonObject(downloadMetadata.getExternalSchema()));

            adgCartridgeClient.uploadData(request)
                    .onSuccess(queryResult -> {
                                log.info("Uploading data from ADG was successful on request: {}",
                                        kafkaRequest.getRequestId());
                                promise.complete(QueryResult.emptyResult());
                            }
                    )
                    .onFailure(fail -> promise.fail(new MpprDatasourceException(
                            String.format("Error unloading data by request %s", request),
                            fail)));
        });
    }
}
