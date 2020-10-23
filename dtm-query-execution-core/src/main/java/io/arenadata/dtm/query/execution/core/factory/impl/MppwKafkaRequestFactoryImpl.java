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
                LocationUriParser.parseKafkaLocationPath(context.getEntity().getExternalTableLocationPath());
        val request = MppwRequest.builder()
                .queryRequest(context.getRequest().getQueryRequest())
                .isLoadStart(true)
                .kafkaParameter(MppwKafkaParameter.builder()
                        .datamart(context.getEntity().getSchema())
                        .sysCn(context.getSysCn())
                        .targetTableName(context.getTargetTable().getTableName())
                        .uploadMetadata(UploadExternalEntityMetadata.builder()
                                .name(context.getEntity().getName())
                                .format(Format.findByName(context.getEntity().getExternalTableFormat()))
                                .locationPath(context.getEntity().getExternalTableLocationPath())
                                .externalSchema(context.getEntity().getExternalTableSchema())
                                .uploadMessageLimit(context.getEntity().getExternalTableUploadMessageLimit())
                                .build())
                        .zookeeperHost(kafkaTopicUri.getHost())
                        .zookeeperPort(kafkaTopicUri.getPort())
                        .topic(kafkaTopicUri.getTopic())
                        .build())
                .build();
        return new MppwRequestContext(request);
    }
}
