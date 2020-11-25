package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppw.load;

import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.AdqmMppwProperties;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class RestLoadInitiatorImpl implements RestLoadInitiator {
    private final WebClient webClient;
    private final AdqmMppwProperties adqmMppwProperties;

    public RestLoadInitiatorImpl(@Qualifier("coreVertx") Vertx vertx,
                                 AdqmMppwProperties adqmMppwProperties) {
        webClient = WebClient.create(vertx);
        this.adqmMppwProperties = adqmMppwProperties;
    }

    @Override
    public Future<Void> initiateLoading(RestMppwKafkaLoadRequest request) {
        try {
            JsonObject data = JsonObject.mapFrom(request);
            Promise<Void> promise = Promise.promise();
            webClient.postAbs(adqmMppwProperties.getRestLoadUrl()).sendJsonObject(data, ar -> {
                if (ar.succeeded()) {
                    HttpResponse<Buffer> response = ar.result();
                    if (response.statusCode() < 400 && response.statusCode() >= 200) {
                        promise.complete();
                    } else {
                        promise.fail(new RuntimeException(String.format("Received HTTP status %s, msg %s", response.statusCode(), response.bodyAsString())));
                    }
                } else {
                    promise.fail(ar.cause());
                }
            });
            return promise.future();
        } catch (Exception e) {
            return Future.failedFuture(e);
        }
    }
}
