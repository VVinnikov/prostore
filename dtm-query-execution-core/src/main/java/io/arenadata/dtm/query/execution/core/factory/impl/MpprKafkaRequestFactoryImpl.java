package io.arenadata.dtm.query.execution.core.factory.impl;

import io.arenadata.dtm.common.plugin.exload.Format;
import io.arenadata.dtm.query.execution.core.dao.zookeeper.ZookeeperKafkaProviderRepository;
import io.arenadata.dtm.query.execution.core.factory.MpprKafkaRequestFactory;
import io.arenadata.dtm.query.execution.core.utils.LocationUriParser;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.DownloadExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.MpprKafkaParameter;
import io.arenadata.dtm.query.execution.plugin.api.request.MpprRequest;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MpprKafkaRequestFactoryImpl implements MpprKafkaRequestFactory {

    private final ZookeeperKafkaProviderRepository zkConnProviderRepository;

    @Autowired
    public MpprKafkaRequestFactoryImpl(ZookeeperKafkaProviderRepository zkConnProviderRepository) {
        this.zkConnProviderRepository = zkConnProviderRepository;
    }

    @Override
    public MpprRequestContext create(EdmlRequestContext context) {
        LocationUriParser.KafkaTopicUri kafkaTopicUri =
                LocationUriParser.parseKafkaLocationPath(context.getDestinationEntity().getExternalTableLocationPath());
        val request = MpprRequest.builder()
                .queryRequest(context.getRequest().getQueryRequest())
                .logicalSchema(context.getLogicalSchema())
                .kafkaParameter(MpprKafkaParameter.builder()
                        .datamart(context.getSourceEntity().getSchema())
                        .dmlSubquery(context.getDmlSubquery())
                        .downloadMetadata(DownloadExternalEntityMetadata.builder()
                                .name(context.getDestinationEntity().getName())
                                .format(Format.findByName(context.getDestinationEntity().getExternalTableFormat()))
                                .externalSchema(context.getDestinationEntity().getExternalTableSchema())
                                .locationPath(context.getDestinationEntity().getExternalTableLocationPath())
                                .chunkSize(context.getDestinationEntity().getExternalTableDownloadChunkSize())
                                .build())
                        .zookeeperHost(kafkaTopicUri.getHost())
                        .zookeeperPort(kafkaTopicUri.getPort())
                        .topic(kafkaTopicUri.getTopic())
                        .build())
                .build();
        return new MpprRequestContext(request);
    }
}
