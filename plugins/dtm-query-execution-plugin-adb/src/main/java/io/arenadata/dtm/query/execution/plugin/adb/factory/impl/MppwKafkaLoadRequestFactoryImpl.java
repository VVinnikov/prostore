package io.arenadata.dtm.query.execution.plugin.adb.factory.impl;

import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.query.execution.plugin.adb.configuration.properties.MppwProperties;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MppwKafkaLoadRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwKafkaLoadRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.UploadExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwPluginRequest;
import lombok.val;
import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class MppwKafkaLoadRequestFactoryImpl implements MppwKafkaLoadRequestFactory {

    private final List<String> excludeSystemFields = Arrays.asList(MetadataSqlFactoryImpl.SYS_FROM_ATTR,
            MetadataSqlFactoryImpl.SYS_TO_ATTR);

    @Override
    public MppwKafkaLoadRequest create(MppwPluginRequest request, String server, MppwProperties mppwProperties) {
        val uploadMeta = (UploadExternalEntityMetadata) request
            .getKafkaParameter().getUploadMetadata();
        val kafkaParam = request.getKafkaParameter();
        val schema = new Schema.Parser().parse(uploadMeta.getExternalSchema());
        val reqId = request.getRequestId().toString();
        return MppwKafkaLoadRequest.builder()
            .requestId(reqId)
            .datamart(kafkaParam.getDatamart())
            .tableName(kafkaParam.getDestinationTableName())
            .writableExtTableName(MetadataSqlFactoryImpl.WRITABLE_EXT_TABLE_PREF + reqId)
            .columns(getColumns(schema))
            .schema(schema)
            .brokers(request.getKafkaParameter().getBrokers().stream()
                    .map(KafkaBrokerInfo::getAddress)
                    .collect(Collectors.joining(",")))
            .consumerGroup(mppwProperties.getConsumerGroup())
            .timeout(mppwProperties.getStopTimeoutMs())
            .topic(request.getKafkaParameter().getTopic())
            .uploadMessageLimit(mppwProperties.getDefaultMessageLimit())
            .server(server)
            .build();
    }

    private List<String> getColumns(Schema schema) {
        return schema.getFields().stream()
            .map(Schema.Field::name)
            .filter(field -> excludeSystemFields.stream()
                .noneMatch(sysName -> sysName.equals(field)))
            .collect(Collectors.toList());
    }
}
