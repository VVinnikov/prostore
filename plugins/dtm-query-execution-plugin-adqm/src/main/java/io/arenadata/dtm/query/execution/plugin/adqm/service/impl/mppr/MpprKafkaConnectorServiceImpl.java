package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppr;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.ConnectorProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.MpprKafkaConnectorRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.service.MpprKafkaConnectorService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.net.HttpURLConnection;

@Service
@Slf4j
public class MpprKafkaConnectorServiceImpl implements MpprKafkaConnectorService {

    private final ConnectorProperties connectorProperties;
    private final WebClient client;

    @Autowired
    public MpprKafkaConnectorServiceImpl(ConnectorProperties connectorProperties,
                                         @Qualifier("adqmWebClient") WebClient webClient) {
        this.connectorProperties = connectorProperties;
        this.client = webClient;
    }

    @Override
    public void call(MpprKafkaConnectorRequest request, Handler<AsyncResult<QueryResult>> handler) {
        log.debug("Calling MpprKafkaConnector with parameters: host = {}, port = {}, url = {}, request = {}",
                connectorProperties.getHost(), connectorProperties.getPort(), connectorProperties.getUrl(), request);
        client.post(connectorProperties.getPort(),
                connectorProperties.getHost(),
                connectorProperties.getUrl())
                .sendJson(request, ar -> {
                    if (ar.succeeded()) {
                        if (ar.result().statusCode() == HttpURLConnection.HTTP_OK) {
                            handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                        } else {
                            handler.handle(Future.failedFuture(ar.cause()));
                        }
                    } else {
                        handler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }
}
