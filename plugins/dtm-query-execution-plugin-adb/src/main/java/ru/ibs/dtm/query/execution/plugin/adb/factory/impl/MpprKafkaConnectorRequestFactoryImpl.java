package ru.ibs.dtm.query.execution.plugin.adb.factory.impl;

import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.adb.dto.MpprKafkaConnectorRequest;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MpprKafkaConnectorRequestFactory;
import ru.ibs.dtm.query.execution.plugin.api.dto.MpprKafkaRequest;

@Component
public class MpprKafkaConnectorRequestFactoryImpl implements MpprKafkaConnectorRequestFactory {

  @Override
  public MpprKafkaConnectorRequest create(MpprKafkaRequest mpprKafkaRequest,
                                          String enrichedQuery) {
    QueryRequest queryRequest = mpprKafkaRequest.getQueryRequest();
    return new MpprKafkaConnectorRequest(
      queryRequest.getSql(),
      queryRequest.getDatamartMnemonic(),
      enrichedQuery,
      mpprKafkaRequest.getZookeeperHost(),
      String.valueOf(mpprKafkaRequest.getZookeeperPort()),
      mpprKafkaRequest.getTopic(),
      mpprKafkaRequest.getQueryExloadParam().getChunkSize());
  }
}
