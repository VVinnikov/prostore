package ru.ibs.dtm.query.execution.plugin.adqm.factory.impl;

import lombok.val;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.adqm.dto.MpprKafkaConnectorRequest;
import ru.ibs.dtm.query.execution.plugin.adqm.factory.MpprKafkaConnectorRequestFactory;
import ru.ibs.dtm.query.execution.plugin.api.mppr.kafka.DownloadExternalEntityMetadata;
import ru.ibs.dtm.query.execution.plugin.api.request.MpprRequest;

@Component
public class MpprKafkaConnectorRequestFactoryImpl implements MpprKafkaConnectorRequestFactory {

    @Override
    public MpprKafkaConnectorRequest create(MpprRequest mpprRequest,
                                            String enrichedQuery) {
        QueryRequest queryRequest = mpprRequest.getQueryRequest();
        val kafkaParam = mpprRequest.getKafkaParameter();
        val downloadMetadata =
                (DownloadExternalEntityMetadata) mpprRequest.getKafkaParameter().getDownloadMetadata();
        return MpprKafkaConnectorRequest.builder()
                .table(queryRequest.getSql())
                .sql(enrichedQuery)
                .datamart(queryRequest.getDatamartMnemonic())
                .zookeeperHost(kafkaParam.getZookeeperHost())
                .zookeeperPort(String.valueOf(kafkaParam.getZookeeperPort()))
                .kafkaTopic(kafkaParam.getTopic())
                .chunkSize(downloadMetadata.getChunkSize())
                .build();
    }
}
