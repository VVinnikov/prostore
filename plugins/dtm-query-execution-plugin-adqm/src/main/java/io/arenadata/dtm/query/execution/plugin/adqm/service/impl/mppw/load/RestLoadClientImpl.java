package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppw.load;

import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
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
public class RestLoadClientImpl implements RestLoadClient {
    private final WebClient webClient;
    private final MppwProperties mppwProperties;

    public RestLoadClientImpl(@Qualifier("coreVertx") Vertx vertx,
                              MppwProperties mppwProperties) {
        webClient = WebClient.create(vertx);
        this.mppwProperties = mppwProperties;
    }

    @Override
    public Future<Void> initiateLoading(RestMppwKafkaLoadRequest request) {
        try {
            JsonObject data = JsonObject.mapFrom(request);
            Promise<Void> promise = Promise.promise();
            webClient.postAbs(mppwProperties.getRestStartLoadUrl()).sendJsonObject(data, ar -> {
                if (ar.succeeded()) {
                    HttpResponse<Buffer> response = ar.result();
                    if (response.statusCode() < 400 && response.statusCode() >= 200) {
                        promise.complete();
                    } else {
                        promise.fail(new DataSourceException(String.format("Received HTTP status %s, msg %s",
                                response.statusCode(),
                                response.bodyAsString())));
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

    @Override
    public Future<Void> stopLoading(RestMppwKafkaStopRequest request) {
        try {
            JsonObject data = JsonObject.mapFrom(request);
            Promise<Void> promise = Promise.promise();
            webClient.postAbs(mppwProperties.getRestStopLoadUrl()).sendJsonObject(data, ar -> {
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
