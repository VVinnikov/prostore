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
                .hotDelta(context.getRequest().getQueryLoadParam().getDeltaHot())
                .datamart(context.getRequest().getQueryLoadParam().getDatamart())
                .tableName(context.getRequest().getQueryLoadParam().getTableName())
                .zookeeperHost(context.getRequest().getZookeeperHost())
                .zookeeperPort(context.getRequest().getZookeeperPort())
                .kafkaTopic(context.getRequest().getTopic())
                .consumerGroup(mppwProperties.getConsumerGroup())
                .format(context.getRequest().getQueryLoadParam().getFormat().getName())
                .schema(new Schema.Parser().parse(context.getRequest().getSchema().encode()))
                .messageProcessingLimit(context.getRequest().getQueryLoadParam().getMessageLimit())
                .build();
    }
}
