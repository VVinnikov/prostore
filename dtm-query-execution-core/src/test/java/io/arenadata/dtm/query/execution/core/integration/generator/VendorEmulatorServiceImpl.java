package io.arenadata.dtm.query.execution.core.integration.generator;

import io.vertx.core.Future;
import io.vertx.ext.web.client.WebClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class VendorEmulatorServiceImpl implements VendorEmulatorService {

    @Autowired
    @Qualifier("itTestWebClient")
    private WebClient webClient;

    @Override
    public Future<Void> generateData(String host, int port, Object loadRequest) {
        return Future.future(promise ->
                webClient.post(port, host, "/vendor-emulator/unload/generated-data")
                        .sendJson(loadRequest, ar -> {
                            if (ar.succeeded()) {
                                promise.complete();
                            } else {
                                promise.fail(ar.cause());
                            }
                        }));
    }
}
