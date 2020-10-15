package ru.ibs.dtm.query.execution.core.factory.impl;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.query.execution.core.factory.MpprKafkaRequestFactory;
import ru.ibs.dtm.query.execution.core.utils.LocationUriParser;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppr.kafka.DownloadExternalEntityMetadata;
import ru.ibs.dtm.query.execution.plugin.api.mppr.kafka.MpprKafkaParameter;
import ru.ibs.dtm.query.execution.plugin.api.request.MpprRequest;

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
