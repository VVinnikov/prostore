package ru.ibs.dtm.query.execution.core.factory.impl;

import lombok.val;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.query.execution.core.factory.MppwKafkaRequestFactory;
import ru.ibs.dtm.query.execution.core.utils.LocationUriParser;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaParameter;
import ru.ibs.dtm.query.execution.plugin.api.mppw.kafka.UploadExternalEntityMetadata;
import ru.ibs.dtm.query.execution.plugin.api.request.MppwRequest;

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
                                .format(Format.valueOf(context.getEntity().getExternalTableFormat()))
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
