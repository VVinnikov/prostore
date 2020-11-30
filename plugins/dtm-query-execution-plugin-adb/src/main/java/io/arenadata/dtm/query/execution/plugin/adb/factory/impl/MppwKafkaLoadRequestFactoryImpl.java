package io.arenadata.dtm.query.execution.plugin.adb.factory.impl;

import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.query.execution.plugin.adb.configuration.properties.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MppwKafkaLoadRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaLoadRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.UploadExternalEntityMetadata;
import lombok.val;
import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

import java.util.stream.Collectors;

@Component
public class MppwKafkaLoadRequestFactoryImpl implements MppwKafkaLoadRequestFactory {

    @Override
    public MppwKafkaLoadRequest create(MppwRequestContext context, String server, MppwProperties mppwProperties) {
        val uploadMeta = (UploadExternalEntityMetadata) context.getRequest()
                .getKafkaParameter().getUploadMetadata();
        val kafkaParam = context.getRequest().getKafkaParameter();
        val schema = new Schema.Parser().parse(uploadMeta.getExternalSchema());
        val reqId = context.getRequest().getQueryRequest().getRequestId().toString();
        return MppwKafkaLoadRequest.builder()
                .requestId(reqId)
                .datamart(kafkaParam.getDatamart())
                .tableName(kafkaParam.getDestinationTableName())
                .writableExtTableName(MetadataSqlFactoryImpl.WRITABLE_EXT_TABLE_PREF + reqId)
                .columns(schema.getFields().stream().map(Schema.Field::name).filter(field -> !field.contains("sys")).collect(Collectors.toList()))
                .schema(schema)
                .brokers(context.getRequest().getKafkaParameter().getBrokers().stream().map(KafkaBrokerInfo::getAddress).collect(Collectors.joining(",")))
                .consumerGroup(mppwProperties.getConsumerGroup())
                .timeout(mppwProperties.getStopTimeoutMs())
                .topic(context.getRequest().getKafkaParameter().getTopic())
                .uploadMessageLimit(mppwProperties.getDefaultMessageLimit())
                .server(server)
                .build();
    }
}
