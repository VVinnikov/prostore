package io.arenadata.dtm.query.execution.core.factory.impl;

import io.arenadata.dtm.common.plugin.exload.Format;
import io.arenadata.dtm.query.execution.core.factory.MppwKafkaRequestFactory;
import io.arenadata.dtm.query.execution.core.utils.LocationUriParser;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaParameter;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.UploadExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwRequest;
import lombok.val;
import org.springframework.stereotype.Component;

@Component
public class MppwKafkaRequestFactoryImpl implements MppwKafkaRequestFactory {

    @Override
    public MppwRequestContext create(EdmlRequestContext context) {
        LocationUriParser.KafkaTopicUri kafkaTopicUri =
                LocationUriParser.parseKafkaLocationPath(context.getSourceEntity().getExternalTableLocationPath());
        val request = MppwRequest.builder()
                .queryRequest(context.getRequest().getQueryRequest())
                .isLoadStart(true)
                .kafkaParameter(MppwKafkaParameter.builder()
                        .datamart(context.getSourceEntity().getSchema())
                        .sysCn(context.getSysCn())
                        .targetTableName(context.getDestinationEntity().getName())
                        .uploadMetadata(UploadExternalEntityMetadata.builder()
                                .name(context.getSourceEntity().getName())
                                .format(Format.findByName(context.getSourceEntity().getExternalTableFormat()))
                                .locationPath(context.getSourceEntity().getExternalTableLocationPath())
                                .externalSchema(context.getSourceEntity().getExternalTableSchema())
                                .uploadMessageLimit(context.getSourceEntity().getExternalTableUploadMessageLimit())
                                .build())
                        .zookeeperHost(kafkaTopicUri.getHost())
                        .zookeeperPort(kafkaTopicUri.getPort())
                        .topic(kafkaTopicUri.getTopic())
                        .build())
                .build();
        return new MppwRequestContext(request);
    }
}
