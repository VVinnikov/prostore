package io.arenadata.dtm.query.execution.plugin.adb.factory.impl;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.adb.dto.MpprKafkaConnectorRequest;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MpprKafkaConnectorRequestFactory;
import io.arenadata.dtm.query.execution.plugin.api.mppr.kafka.DownloadExternalEntityMetadata;
import io.arenadata.dtm.query.execution.plugin.api.request.MpprRequest;
import lombok.val;
import org.springframework.stereotype.Component;

@Component
public class MpprKafkaConnectorRequestFactoryImpl implements MpprKafkaConnectorRequestFactory {

    @Override
    public MpprKafkaConnectorRequest create(MpprRequest mpprRequest,
                                            String enrichedQuery) {
        QueryRequest queryRequest = mpprRequest.getQueryRequest();
        val downloadMetadata =
                (DownloadExternalEntityMetadata) mpprRequest.getKafkaParameter().getDownloadMetadata();
        return MpprKafkaConnectorRequest.builder()
                .table(queryRequest.getSql())
                .sql(enrichedQuery)
                .datamart(queryRequest.getDatamartMnemonic())
                .kafkaBrokers(mpprRequest.getKafkaParameter().getBrokers())
                .kafkaTopic(mpprRequest.getKafkaParameter().getTopic())
                .chunkSize(downloadMetadata.getChunkSize())
                .build();
    }
}
