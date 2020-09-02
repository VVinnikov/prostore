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
        RestLoadRequest request = new RestLoadRequest();
        request.setRequestId(context.getRequest().getQueryRequest().getRequestId().toString());
        request.setHotDelta(context.getRequest().getQueryLoadParam().getDeltaHot());
        request.setDatamart(context.getRequest().getQueryLoadParam().getDatamart());
        request.setTableName(context.getRequest().getQueryLoadParam().getTableName());
        request.setZookeeperHost(context.getRequest().getZookeeperHost());
        request.setZookeeperPort(context.getRequest().getZookeeperPort());
        request.setKafkaTopic(context.getRequest().getTopic());
        request.setConsumerGroup(mppwProperties.getConsumerGroup());
        request.setFormat(context.getRequest().getQueryLoadParam().getFormat().getName());
        request.setSchema(new Schema.Parser().parse(context.getRequest().getSchema().encode()));
        request.setPath(context.getRequest().getLoadStart() ?
                mppwProperties.getStartLoadUrl() : mppwProperties.getStopLoadUrl());
        return request;
    }
}
