package ru.ibs.dtm.query.execution.plugin.adb.factory.impl;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.plugin.adb.configuration.properties.MppwProperties;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MppwRestLoadRequestFactory;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.RestLoadRequest;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;

@Component
public class MppwRestLoadRequestFactoryImpl implements MppwRestLoadRequestFactory {

    private final MppwProperties mppwProperties;

    @Autowired
    public MppwRestLoadRequestFactoryImpl(MppwProperties mppwProperties) {
        this.mppwProperties = mppwProperties;
    }

    @Override
    public RestLoadRequest create(MppwRequestContext context) {
        return RestLoadRequest.builder()
                .requestId(context.getRequest().getQueryRequest().getRequestId().toString())
                .sysCn(context.getRequest().getKafkaParameter().getSysCn())
                .datamart(context.getRequest().getKafkaParameter().getDatamart())
                .tableName(context.getRequest().getKafkaParameter().getTargetTableName())
                .zookeeperHost(context.getRequest().getKafkaParameter().getUploadMetadata().getZookeeperHost())
                .zookeeperPort(context.getRequest().getKafkaParameter().getUploadMetadata().getZookeeperPort())
                .kafkaTopic(context.getRequest().getKafkaParameter().getUploadMetadata().getTopic())
                .consumerGroup(mppwProperties.getConsumerGroup())
                .format(context.getRequest().getKafkaParameter().getUploadMetadata().getExternalTableFormat().getName())
                .schema(new Schema.Parser().parse(context.getRequest().getKafkaParameter().getUploadMetadata().getExternalTableSchema()))
                .messageProcessingLimit(context.getRequest().getKafkaParameter().getUploadMetadata().getExternalTableUploadMessageLimit())
                .build();
    }
}
