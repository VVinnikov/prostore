package ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.handler;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.plugin.adb.configuration.properties.MppwProperties;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.AdbMppwDataTransferService;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaRequestContext;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.RestLoadRequest;

@Component("adbMppwTransferDataHandler")
@Slf4j
public class AdbMppwTransferDataHandler implements AdbMppwHandler {

    private final WebClient webClient;
    private final AdbMppwDataTransferService mppwDataTransferService;
    private final MppwProperties mppwProperties;

    @Autowired
    public AdbMppwTransferDataHandler(@Qualifier("adbWebClient") WebClient webClient,
                                      AdbMppwDataTransferService mppwDataTransferService,
                                      MppwProperties mppwProperties) {
        this.webClient = webClient;
        this.mppwDataTransferService = mppwDataTransferService;
        this.mppwProperties = mppwProperties;
    }

    @Override
    public Future<Void> handle(MppwKafkaRequestContext requestContext) {
        return sendLoadingRequest(requestContext.getRestLoadRequest())
                .compose(s -> Future.future((Promise<Void> p) ->
                        mppwDataTransferService.execute(requestContext.getMppwTransferDataRequest(), p)));
    }

    private Future<Void> sendLoadingRequest(RestLoadRequest request) {
        try {
            JsonObject data = JsonObject.mapFrom(request);
            Promise<Void> promise = Promise.promise();
            log.debug("Send request to emulator-writer: [{}]", request);
            webClient.postAbs(mppwProperties.getStartLoadUrl()).sendJsonObject(data, ar -> {
                if (ar.succeeded()) {
                    HttpResponse<Buffer> response = ar.result();
                    if (response.statusCode() < 400 && response.statusCode() >= 200) {
                        promise.complete();
                    } else {
                        promise.fail(new RuntimeException(String.format("Received HTTP status %s, msg %s",
                                response.statusCode(), response.bodyAsString())));
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
