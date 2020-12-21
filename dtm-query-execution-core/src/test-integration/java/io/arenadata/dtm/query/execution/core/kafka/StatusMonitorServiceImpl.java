package io.arenadata.dtm.query.execution.core.kafka;

import io.arenadata.dtm.common.status.kafka.StatusRequest;
import io.arenadata.dtm.common.status.kafka.StatusResponse;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class StatusMonitorServiceImpl implements StatusMonitorService {

    @Autowired
    @Qualifier("itTestWebClient")
    private WebClient webClient;

    @Override
    public Future<StatusResponse> getTopicStatus(String host, int port, StatusRequest statusRequest) {
        return Future.future(promise ->
                webClient.post(port, host, "/status")
                        .sendJson(statusRequest, ar -> {
                            if (ar.succeeded()) {
                                final HttpResponse<Buffer> httpResponse = ar.result();
                                promise.complete(httpResponse.bodyAsJson(StatusResponse.class));
                            } else {
                                promise.fail(ar.cause());
                            }
                        }));
    }
}
