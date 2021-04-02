package io.arenadata.dtm.query.execution.plugin.adqm.mppr.kafka.factory.impl;

import io.arenadata.dtm.query.execution.plugin.adqm.mppr.kafka.dto.MpprKafkaConnectorRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.mppr.kafka.factory.MpprKafkaConnectorRequestFactory;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.DownloadExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.MpprKafkaRequest;
import lombok.val;
import org.springframework.stereotype.Component;

@Component
public class MpprKafkaConnectorRequestFactoryImpl implements MpprKafkaConnectorRequestFactory {

    @Override
    public MpprKafkaConnectorRequest create(MpprKafkaRequest request,
                                            String enrichedQuery) {
        val downloadMetadata =
                (DownloadExternalEntityMetadata) request.getDownloadMetadata();
        return MpprKafkaConnectorRequest.builder()
                .table(request.getSql())
                .sql(enrichedQuery)
                .datamart(request.getDatamartMnemonic())
                .kafkaBrokers(request.getBrokers())
                .kafkaTopic(request.getTopic())
                .chunkSize(downloadMetadata.getChunkSize())
                .avroSchema(downloadMetadata.getExternalSchema())
                .metadata(request.getMetadata())
                .build();
    }
}
