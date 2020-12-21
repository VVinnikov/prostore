package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppw.load;

import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.AdqmMppwProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.mppw.RestMppwKafkaLoadRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.mppw.RestMppwKafkaStopRequest;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RestLoadClientImpl implements RestLoadClient {
    private final WebClient webClient;
    private final AdqmMppwProperties mppwProperties;

    @Autowired
    public RestLoadClientImpl(@Qualifier("coreVertx") Vertx vertx,
                              AdqmMppwProperties mppwProperties) {
        this.webClient = WebClient.create(vertx);
        this.mppwProperties = mppwProperties;
    }

    @Override
    public Future<Void> initiateLoading(RestMppwKafkaLoadRequest request) {
        return Future.future(promise -> {
            JsonObject data = JsonObject.mapFrom(request);
            executePostRequest(mppwProperties.getRestStartLoadUrl(), data)
                    .onComplete(promise);
        });
    }

    @Override
    public Future<Void> stopLoading(RestMppwKafkaStopRequest request) {
        return Future.future(promise -> {
            JsonObject data = JsonObject.mapFrom(request);
            executePostRequest(mppwProperties.getRestStopLoadUrl(), data)
                    .onComplete(promise);
        });
    }

    private Future<Void> executePostRequest(String uri, JsonObject data) {
        return Future.future(promise -> {
            log.debug("Send request to [{}] data [{}]", uri, data);
            webClient.postAbs(uri).sendJsonObject(data, ar -> {
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
        });
    }
}
