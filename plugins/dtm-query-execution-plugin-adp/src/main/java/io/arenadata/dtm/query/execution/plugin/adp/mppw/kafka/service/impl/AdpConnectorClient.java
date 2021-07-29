package io.arenadata.dtm.query.execution.plugin.adp.mppw.kafka.service.impl;

import io.arenadata.dtm.query.execution.plugin.adp.base.properties.AdpMppwProperties;
import io.arenadata.dtm.query.execution.plugin.adp.mppw.dto.AdpConnectorMpprRequest;
import io.arenadata.dtm.query.execution.plugin.adp.mppw.dto.AdpConnectorMppwStartRequest;
import io.arenadata.dtm.query.execution.plugin.adp.mppw.dto.AdpConnectorMppwStopRequest;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AdpConnectorClient {
    private final WebClient webClient;
    private final AdpMppwProperties mppwProperties;

    public AdpConnectorClient(@Qualifier("coreVertx") Vertx vertx,
                              AdpMppwProperties mppwProperties) {
        this.webClient = WebClient.create(vertx);
        this.mppwProperties = mppwProperties;
    }

    public Future<Void> startMppw(AdpConnectorMppwStartRequest request) {
        return Future.future(event -> {
            JsonObject data = JsonObject.mapFrom(request);
            executePostRequest(mppwProperties.getRestStartLoadUrl(), data)
                    .onComplete(event);
        });


    }

    public Future<Void> stopMppw(AdpConnectorMppwStopRequest request) {
        return Future.future(event -> {
            JsonObject data = JsonObject.mapFrom(request);
            executePostRequest(mppwProperties.getRestStopLoadUrl(), data)
                    .onComplete(event);
        });
    }

    public Future<Void> runMppr(AdpConnectorMpprRequest request) {
        return Future.failedFuture(new UnsupportedOperationException("Not implemented"));
    }

    private Future<Void> executePostRequest(String uri, JsonObject data) {
        return Future.future(promise -> {
            log.debug("[ADP] Request[POST] to [{}] trying to send, data: [{}]", uri, data);
            webClient.postAbs(uri)
                    .sendJsonObject(data)
                    .onSuccess(response -> {
                        if (response.statusCode() < 400 && response.statusCode() >= 200) {
                            log.debug("[ADP] Request[POST] to [{}] succeeded", uri);
                            promise.complete();
                        } else {
                            log.error("[ADP] Request[POST] to [{}] failed with error [{}] response [{}]",
                                    uri, response.statusCode(), response.body());
                            promise.fail(new DataSourceException(
                                    String.format("Request[POST] to [%s] status [%s], msg [%s]",
                                            uri, response.statusCode(), response.bodyAsString())));
                        }
                    })
                    .onFailure(t -> {
                        log.error("[ADP] Request[POST] to [{}] failed with exception", uri, t);
                        promise.fail(new DataSourceException(
                                String.format("Request[POST] to [%s] failed", uri), t));
                    });
        });
    }
}
