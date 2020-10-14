package ru.ibs.dtm.query.execution.core.factory.impl;

import lombok.val;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.plugin.exload.Format;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.core.factory.MppwKafkaRequestFactory;
import ru.ibs.dtm.query.execution.core.utils.LocationUriParser;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppw.parameter.KafkaParameter;
import ru.ibs.dtm.query.execution.plugin.api.mppw.parameter.UploadExternalMetadata;
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
                .kafkaParameter(KafkaParameter.builder()
                        .datamart(context.getEntity().getSchema())
                        .sysCn(context.getSysCn())
                        .targetTableName(context.getTargetTable().getTableName())
                        .uploadMetadata(UploadExternalMetadata.builder()
                                .name(context.getEntity().getName())
                                .externalTableFormat(Format.valueOf(context.getEntity().getExternalTableFormat()))
                                .externalTableLocationPath(context.getEntity().getExternalTableLocationPath())
                                .externalTableSchema(context.getEntity().getExternalTableSchema())
                                .externalTableUploadMessageLimit(context.getEntity().getExternalTableUploadMessageLimit())
                                .zookeeperHost(kafkaTopicUri.getHost())
                                .zookeeperPort(kafkaTopicUri.getPort())
                                .topic(kafkaTopicUri.getTopic())
                                .build())
                        .build())
                .build();
        return new MppwRequestContext(request);
    }
}
