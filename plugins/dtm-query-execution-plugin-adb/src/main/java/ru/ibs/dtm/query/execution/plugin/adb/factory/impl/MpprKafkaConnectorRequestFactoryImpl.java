package ru.ibs.dtm.query.execution.plugin.adb.factory.impl;

import lombok.val;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.adb.dto.MpprKafkaConnectorRequest;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MpprKafkaConnectorRequestFactory;
import ru.ibs.dtm.query.execution.plugin.api.mppr.kafka.DownloadExternalEntityMetadata;
import ru.ibs.dtm.query.execution.plugin.api.request.MpprRequest;

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
                .zookeeperHost(mpprRequest.getKafkaParameter().getZookeeperHost())
                .zookeeperPort(String.valueOf(mpprRequest.getKafkaParameter().getZookeeperPort()))
                .kafkaTopic(mpprRequest.getKafkaParameter().getTopic())
                .chunkSize(downloadMetadata.getChunkSize())
                .build();
    }
}
