package ru.ibs.dtm.query.execution.plugin.adb.factory.impl;

import lombok.val;
import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.plugin.adb.configuration.properties.MppwProperties;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MppwRestLoadRequestFactory;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.RestLoadRequest;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppw.kafka.UploadExternalEntityMetadata;

@Component
public class MppwRestLoadRequestFactoryImpl implements MppwRestLoadRequestFactory {

    private final MppwProperties mppwProperties;

    @Autowired
    public MppwRestLoadRequestFactoryImpl(MppwProperties mppwProperties) {
        this.mppwProperties = mppwProperties;
    }

    @Override
    public RestLoadRequest create(MppwRequestContext context) {
        val uploadMeta = (UploadExternalEntityMetadata) context.getRequest()
                .getKafkaParameter().getUploadMetadata();
        val kafkaParam = context.getRequest().getKafkaParameter();
        return RestLoadRequest.builder()
                .requestId(context.getRequest().getQueryRequest().getRequestId().toString())
                .sysCn(kafkaParam.getSysCn())
                .datamart(kafkaParam.getDatamart())
                .tableName(kafkaParam.getTargetTableName())
                .zookeeperHost(kafkaParam.getZookeeperHost())
                .zookeeperPort(kafkaParam.getZookeeperPort())
                .kafkaTopic(kafkaParam.getTopic())
                .consumerGroup(mppwProperties.getConsumerGroup())
                .format(uploadMeta.getFormat().getName())
                .schema(new Schema.Parser().parse(uploadMeta.getExternalSchema()))
                .messageProcessingLimit(uploadMeta.getUploadMessageLimit())
                .build();
    }
}
