package io.arenadata.dtm.query.execution.core.factory.impl;

import io.arenadata.dtm.common.plugin.exload.Format;
import io.arenadata.dtm.query.execution.core.factory.MpprKafkaRequestFactory;
import io.arenadata.dtm.query.execution.core.utils.LocationUriParser;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.DownloadExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.MpprKafkaParameter;
import io.arenadata.dtm.query.execution.plugin.api.request.MpprRequest;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MpprKafkaRequestFactoryImpl implements MpprKafkaRequestFactory {

    @Override
    public MpprRequestContext create(EdmlRequestContext context) {
        LocationUriParser.KafkaTopicUri kafkaTopicUri =
                LocationUriParser.parseKafkaLocationPath(context.getEntity().getExternalTableLocationPath());
        val request = MpprRequest.builder()
                .queryRequest(context.getRequest().getQueryRequest())
                .logicalSchema(context.getLogicalSchema())
                .kafkaParameter(MpprKafkaParameter.builder()
                        .datamart(context.getEntity().getSchema())
                        .dmlSubquery(context.getDmlSubquery())
                        .downloadMetadata(DownloadExternalEntityMetadata.builder()
                                .name(context.getEntity().getName())
                                .format(Format.findByName(context.getEntity().getExternalTableFormat()))
                                .externalSchema(context.getEntity().getExternalTableSchema())
                                .locationPath(context.getEntity().getExternalTableLocationPath())
                                .chunkSize(context.getEntity().getExternalTableDownloadChunkSize())
                                .build())
                        .zookeeperHost(kafkaTopicUri.getHost())
                        .zookeeperPort(kafkaTopicUri.getPort())
                        .topic(kafkaTopicUri.getTopic())
                        .build())
                .build();
        return new MpprRequestContext(request);
    }
}
