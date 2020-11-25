package io.arenadata.dtm.query.execution.plugin.adqm.factory.impl;

import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.AdqmMppwProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.factory.AdqmRestMppwKafkaRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppw.load.RestMppwKafkaLoadRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.UploadExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwRequest;
import lombok.val;
import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AdqmRestMppwKafkaRequestFactoryImpl implements AdqmRestMppwKafkaRequestFactory {

    private final AdqmMppwProperties adqmMppwProperties;

    @Autowired
    public AdqmRestMppwKafkaRequestFactoryImpl(AdqmMppwProperties adqmMppwProperties) {
        this.adqmMppwProperties = adqmMppwProperties;
    }

    @Override
    public RestMppwKafkaLoadRequest create(MppwRequest mppwRequest) {
        val uploadMeta = (UploadExternalEntityMetadata)
                mppwRequest.getKafkaParameter().getUploadMetadata();
        return RestMppwKafkaLoadRequest.builder()
                .requestId(mppwRequest.getQueryRequest().getRequestId().toString())
                .datamart(mppwRequest.getKafkaParameter().getDatamart())
                .tableName(mppwRequest.getKafkaParameter().getTargetTableName())
                .kafkaTopic(mppwRequest.getKafkaParameter().getTopic())
                .kafkaBrokers(mppwRequest.getKafkaParameter().getBrokers())
                .hotDelta(mppwRequest.getKafkaParameter().getSysCn())
                .consumerGroup(adqmMppwProperties.getRestLoadConsumerGroup())
                .format(uploadMeta.getFormat().getName())
                .schema(new Schema.Parser().parse(uploadMeta.getExternalSchema()))
                .messageProcessingLimit(uploadMeta.getUploadMessageLimit() == null ? 0 : uploadMeta.getUploadMessageLimit())
                .build();
    }
}
