package io.arenadata.dtm.query.execution.core.factory.impl;

import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.common.plugin.exload.Format;
import io.arenadata.dtm.kafka.core.repository.ZookeeperKafkaProviderRepository;
import io.arenadata.dtm.query.execution.core.exception.UnreachableLocationException;
import io.arenadata.dtm.query.execution.core.factory.MppwKafkaRequestFactory;
import io.arenadata.dtm.query.execution.core.utils.LocationUriParser;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaParameter;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.UploadExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwPluginRequest;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class MppwKafkaRequestFactoryImpl implements MppwKafkaRequestFactory {

    private final ZookeeperKafkaProviderRepository zkConnProviderRepository;
    private final Vertx vertx;
    private final LocationUriParser locationUriParser;

    @Autowired
    public MppwKafkaRequestFactoryImpl(@Qualifier("coreVertx") Vertx vertx,
                                       @Qualifier("mapZkKafkaProviderRepository") ZookeeperKafkaProviderRepository zkConnProviderRepository,
                                       LocationUriParser locationUriParser) {
        this.zkConnProviderRepository = zkConnProviderRepository;
        this.vertx = vertx;
        this.locationUriParser = locationUriParser;
    }

    @Override
    public Future<MppwPluginRequest> create(EdmlRequestContext context) {
        return Future.future(promise -> {
            LocationUriParser.KafkaTopicUri kafkaTopicUri =
                    locationUriParser.parseKafkaLocationPath(context.getSourceEntity().getExternalTableLocationPath());
            getBrokers(kafkaTopicUri.getAddress())
                    .map(brokers -> new MppwPluginRequest(
                            context.getRequest().getQueryRequest().getRequestId(),
                            context.getEnvName(),
                            context.getRequest().getQueryRequest().getDatamartMnemonic(),
                            true,
                            MppwKafkaParameter.builder()
                                    .datamart(context.getSourceEntity().getSchema())
                                    .sysCn(context.getSysCn())
                                    .destinationTableName(context.getDestinationEntity().getName())
                                    .uploadMetadata(UploadExternalEntityMetadata.builder()
                                            .name(context.getSourceEntity().getName())
                                            .format(Format.findByName(context.getSourceEntity().getExternalTableFormat()))
                                            .locationPath(context.getSourceEntity().getExternalTableLocationPath())
                                            .externalSchema(context.getSourceEntity().getExternalTableSchema())
                                            .uploadMessageLimit(context.getSourceEntity().getExternalTableUploadMessageLimit())
                                            .build())
                                    .brokers(brokers)
                                    .topic(kafkaTopicUri.getTopic())
                                    .sourceEntity(context.getSourceEntity())
                                    .build()))
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
