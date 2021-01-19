package io.arenadata.dtm.query.execution.core.factory.impl;

import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.common.plugin.exload.Format;
import io.arenadata.dtm.kafka.core.repository.ZookeeperKafkaProviderRepository;
import io.arenadata.dtm.query.execution.core.exception.UnreachableLocationException;
import io.arenadata.dtm.query.execution.core.factory.MpprKafkaRequestFactory;
import io.arenadata.dtm.query.execution.core.utils.LocationUriParser;
import io.arenadata.dtm.query.execution.core.dto.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.DownloadExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.MpprKafkaParameter;
import io.arenadata.dtm.query.execution.plugin.api.request.MpprRequest;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class MpprKafkaRequestFactoryImpl implements MpprKafkaRequestFactory {

    private final ZookeeperKafkaProviderRepository zkConnProviderRepository;
    private final Vertx vertx;
    private final LocationUriParser locationUriParser;

    @Autowired
    public MpprKafkaRequestFactoryImpl(@Qualifier("coreVertx") Vertx vertx,
                                       @Qualifier("mapZkKafkaProviderRepository") ZookeeperKafkaProviderRepository zkConnProviderRepository,
                                       LocationUriParser locationUriParser) {
        this.zkConnProviderRepository = zkConnProviderRepository;
        this.vertx = vertx;
        this.locationUriParser = locationUriParser;
    }

    @Override
    public Future<MpprRequestContext> create(EdmlRequestContext context) {
        return Future.future(promise -> {
            LocationUriParser.KafkaTopicUri kafkaTopicUri =
                    locationUriParser.parseKafkaLocationPath(context.getDestinationEntity().getExternalTableLocationPath());
            getBrokers(kafkaTopicUri.getAddress())
                    .map(brokers ->
                            new MpprRequestContext(
                                    context.getMetrics(),
                                    MpprRequest.builder()
                                    .queryRequest(context.getRequest().getQueryRequest())
                                    .logicalSchema(context.getLogicalSchema())
                                    .destinationEntity(context.getDestinationEntity())
                                    .kafkaParameter(MpprKafkaParameter.builder()
                                            .datamart(context.getSourceEntity().getSchema())
                                            .dmlSubQuery(context.getDmlSubQuery())
                                            .downloadMetadata(DownloadExternalEntityMetadata.builder()
                                                    .name(context.getDestinationEntity().getName())
                                                    .format(Format.findByName(context.getDestinationEntity().getExternalTableFormat()))
                                                    .externalSchema(context.getDestinationEntity().getExternalTableSchema())
                                                    .locationPath(context.getDestinationEntity().getExternalTableLocationPath())
                                                    .chunkSize(context.getDestinationEntity().getExternalTableDownloadChunkSize())
                                                    .build())
                                            .brokers(brokers)
                                            .topic(kafkaTopicUri.getTopic())
                                            .build())
                                    .build(),
                                    context.getDmlSubQuery(), deltaInformations))
                    .onComplete(promise);
        });
    }

    private Future<List<KafkaBrokerInfo>> getBrokers(String connectionString) {
        return Future.future(promise -> this.vertx.executeBlocking(blockingPromise -> {
            try {
                blockingPromise.complete(zkConnProviderRepository.getOrCreate(connectionString).getKafkaBrokers());
            } catch (Exception e) {
                blockingPromise.fail(new UnreachableLocationException(connectionString, e));
            }
        }, promise));
    }
}
