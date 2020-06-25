package ru.ibs.dtm.query.execution.core.factory.impl;

import lombok.val;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.core.factory.MppwKafkaRequestFactory;
import ru.ibs.dtm.query.execution.core.utils.LocationUriParser;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.MppwRequest;

@Component
public class MppwKafkaRequestFactoryImpl implements MppwKafkaRequestFactory {

    @Override
    public MppwRequestContext create(EdmlRequestContext context) {
        val request = new MppwRequest(context.getRequest().getQueryRequest(), context.getLoadParam(), context.getSchema());
        LocationUriParser.KafkaTopicUri kafkaTopicUri = LocationUriParser.parseKafkaLocationPath(context.getLoadParam().getLocationPath());
        request.setTopic(kafkaTopicUri.getTopic());
        request.setZookeeperHost(kafkaTopicUri.getHost());
        request.setZookeeperPort(kafkaTopicUri.getPort());
        return new MppwRequestContext(request);
    }
}
