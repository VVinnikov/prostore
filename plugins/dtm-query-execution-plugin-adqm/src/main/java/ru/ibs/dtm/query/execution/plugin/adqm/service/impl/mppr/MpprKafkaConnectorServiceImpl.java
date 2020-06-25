package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.mppr;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.web.client.WebClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adqm.configuration.properties.ConnectorProperties;
import ru.ibs.dtm.query.execution.plugin.adqm.dto.MpprKafkaConnectorRequest;
import ru.ibs.dtm.query.execution.plugin.adqm.service.MpprKafkaConnectorService;

import java.net.HttpURLConnection;

@Service
public class MpprKafkaConnectorServiceImpl implements MpprKafkaConnectorService {

    private final static Logger LOGGER = LoggerFactory.getLogger(MpprKafkaConnectorServiceImpl.class);
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
        LOGGER.debug("Вызов MpprKafkaConnector с параметрами: host={}, port={}, url={}, request={}",
                connectorProperties.getHost(), connectorProperties.getPort(), connectorProperties.getUrl(), request);
        client.post(connectorProperties.getPort(),
                connectorProperties.getHost(),
                connectorProperties.getUrl())
                .sendJson(request, ar -> {
                    if (ar.succeeded()) {
                        if (ar.result().statusCode() == HttpURLConnection.HTTP_OK) {
                            handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                        } else {
                            LOGGER.error("Ошибка выполнения запроса [{}]", request);
                            handler.handle(Future.failedFuture(ar.result().bodyAsString()));
                        }
                    } else {
                        LOGGER.error("Ошибка выполнения запоса [{}]", request);
                        handler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }
}
