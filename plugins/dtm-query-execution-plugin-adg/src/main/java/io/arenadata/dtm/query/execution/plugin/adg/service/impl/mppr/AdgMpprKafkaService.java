package io.arenadata.dtm.query.execution.plugin.adg.service.impl.mppr;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adg.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.request.TtUploadDataKafkaRequest;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.adg.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.api.exception.MpprDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprPluginRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.DownloadExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.request.MpprRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.MpprKafkaService;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@Service("adgMpprKafkaService")
public class AdgMpprKafkaService implements MpprKafkaService {
    private final QueryEnrichmentService adbQueryEnrichmentService;
    private final AdgCartridgeClient adgCartridgeClient;

    @Override
    public Future<QueryResult> execute(MpprPluginRequest request) {
        return Future.future(promise -> {
            MpprRequest mpprRequest = request.getMpprRequest();
            EnrichQueryRequest enrichQueryRequest = EnrichQueryRequest.generate(mpprRequest.getQueryRequest(),
                    mpprRequest.getLogicalSchema(), request.getSqlNode());
            adbQueryEnrichmentService.enrich(enrichQueryRequest)
                    .compose(enrichQuery -> uploadData(mpprRequest, enrichQuery))
                    .onComplete(promise);
        });
    }

    private Future<QueryResult> uploadData(MpprRequest queryRequest, String sql) {
        return Future.future(promise -> {
            val downloadMetadata =
                    (DownloadExternalEntityMetadata) queryRequest.getKafkaParameter().getDownloadMetadata();
            val request = new TtUploadDataKafkaRequest(
                    sql,
                    queryRequest.getKafkaParameter().getTopic(),
                    downloadMetadata.getChunkSize(),
                    new JsonObject(downloadMetadata.getExternalSchema()));

            adgCartridgeClient.uploadData(request)
                    .onSuccess(queryResult -> {
                                UUID requestId = queryRequest.getQueryRequest().getRequestId();
                                log.info("Uploading data from ADG was successful on request: {}", requestId);
                                promise.complete(QueryResult.emptyResult());
                            }
                    )
                    .onFailure(fail -> promise.fail(new MpprDatasourceException(
                            String.format("Error unloading data by request %s", request),
                            fail)));
        });
    }
}
