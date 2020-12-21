package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppr;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.AdqmMpprProperties;
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

    private final AdqmMpprProperties adqmMpprProperties;
    private final WebClient client;

    @Autowired
    public MpprKafkaConnectorServiceImpl(AdqmMpprProperties adqmMpprProperties,
                                         @Qualifier("adqmWebClient") WebClient webClient) {
        this.adqmMpprProperties = adqmMpprProperties;
        this.client = webClient;
    }

    @Override
    public Future<QueryResult> call(MpprKafkaConnectorRequest request) {
        return Future.future(promise -> {
            log.debug("Calling MpprKafkaConnector with parameters: host = {}, port = {}, url = {}, request = {}",
                    adqmMpprProperties.getHost(),
                    adqmMpprProperties.getPort(),
                    adqmMpprProperties.getUrl(),
                    request);
            client.post(adqmMpprProperties.getPort(),
                    adqmMpprProperties.getHost(),
                    adqmMpprProperties.getUrl())
                    .sendJson(request, ar -> {
                        if (ar.succeeded()) {
                            if (ar.result().statusCode() == HttpURLConnection.HTTP_OK) {
                                promise.complete(QueryResult.emptyResult());
                            } else {
                                promise.fail(ar.cause());
                            }
                        } else {
                            promise.fail(ar.cause());
                        }
                    });
        });
    }
}
