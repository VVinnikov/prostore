package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppr;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.AdqmMpprProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.MpprKafkaConnectorRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.service.MpprKafkaConnectorService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.web.client.WebClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.net.HttpURLConnection;

@Service
public class MpprKafkaConnectorServiceImpl implements MpprKafkaConnectorService {

    private final static Logger LOGGER = LoggerFactory.getLogger(MpprKafkaConnectorServiceImpl.class);
    private final AdqmMpprProperties adqmMpprProperties;
    private final WebClient client;

    @Autowired
    public MpprKafkaConnectorServiceImpl(AdqmMpprProperties adqmMpprProperties,
                                         @Qualifier("adqmWebClient") WebClient webClient) {
        this.adqmMpprProperties = adqmMpprProperties;
        this.client = webClient;
    }

    @Override
    public void call(MpprKafkaConnectorRequest request, Handler<AsyncResult<QueryResult>> handler) {
        LOGGER.debug("Calling MpprKafkaConnector with parameters: host = {}, port = {}, url = {}, request = {}",
                adqmMpprProperties.getHost(), adqmMpprProperties.getPort(), adqmMpprProperties.getUrl(), request);
        client.post(adqmMpprProperties.getPort(),
                adqmMpprProperties.getHost(),
                adqmMpprProperties.getUrl())
                .sendJson(request, ar -> {
                    if (ar.succeeded()) {
                        if (ar.result().statusCode() == HttpURLConnection.HTTP_OK) {
                            handler.handle(Future.succeededFuture(QueryResult.emptyResult()));
                        } else {
                            LOGGER.error("Request execution error [{}]", request);
                            handler.handle(Future.failedFuture(ar.result().bodyAsString()));
                        }
                    } else {
                        LOGGER.error("Query execution error [{}]", request);
                        handler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }
}
