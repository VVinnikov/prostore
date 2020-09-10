package ru.ibs.dtm.kafka.core.service.kafka;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.plugin.status.kafka.KafkaPartitionInfo;
import ru.ibs.dtm.common.status.kafka.StatusRequest;
import ru.ibs.dtm.common.status.kafka.StatusResponse;
import ru.ibs.dtm.kafka.core.configuration.properties.KafkaProperties;

import java.time.ZoneId;
import java.util.Date;

@Component
@Slf4j
public class RestConsumerMonitorImpl implements KafkaConsumerMonitor {
    private final WebClient webClient;
    private final KafkaProperties kafkaProperties;

    public RestConsumerMonitorImpl(@Qualifier("coreVertx") Vertx vertx,
                                   @Qualifier("coreKafkaProperties") KafkaProperties kafkaProperties) {
        this.webClient = WebClient.create(vertx);
        this.kafkaProperties = kafkaProperties;
    }

    @Override
    public Future<KafkaPartitionInfo> getAggregateGroupConsumerInfo(String consumerGroup, String topic) {
        return Future.future((Promise<KafkaPartitionInfo> p) -> {
            StatusRequest request = new StatusRequest(topic, consumerGroup);
            webClient.postAbs(kafkaProperties.getStatusMonitorUrl()).sendJsonObject(JsonObject.mapFrom(request), ar -> {
                if (ar.succeeded()) {
                    HttpResponse<Buffer> response = ar.result();
                    if (response.statusCode() < 400 && response.statusCode() >= 200) {
                        StatusResponse statusResponse;
                        try {
                            statusResponse = response.bodyAsJson(StatusResponse.class);
                        } catch (Exception e) {
                            p.fail(e);
                            return;
                        }
                        KafkaPartitionInfo kafkaPartitionInfo = KafkaPartitionInfo.builder()
                                .consumerGroup(statusResponse.getConsumerGroup())
                                .topic(statusResponse.getTopic())
                                .offset(statusResponse.getConsumerOffset())
                                .end(statusResponse.getProducerOffset())
                                .lastCommitTime(new Date(statusResponse.getLastCommitTime()).toInstant()
                                        .atZone(ZoneId.systemDefault()).toLocalDateTime())
                                .build();
                        p.complete(kafkaPartitionInfo);
                    } else {
                        p.fail(new RuntimeException(String.format("Received HTTP status %s, msg %s", response.statusCode(), response.bodyAsString())));
                    }
                } else {
                    p.fail(ar.cause());
                }
            });
        });
    }
}
