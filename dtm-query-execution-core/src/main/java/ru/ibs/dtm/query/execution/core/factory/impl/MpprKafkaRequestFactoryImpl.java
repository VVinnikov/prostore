package ru.ibs.dtm.query.execution.core.factory.impl;

import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.plugin.exload.QueryExloadParam;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.core.factory.MpprKafkaRequestFactory;
import ru.ibs.dtm.query.execution.core.utils.LocationUriParser;
import ru.ibs.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.MpprRequest;

@Slf4j
@Component
public class MpprKafkaRequestFactoryImpl implements MpprKafkaRequestFactory {

    @Override
    public MpprRequestContext create(QueryRequest queryRequest, QueryExloadParam queryExloadParam, JsonObject schema) {
        val request = new MpprRequest(queryRequest, queryExloadParam, schema);
        LocationUriParser.KafkaTopicUri kafkaTopicUri = LocationUriParser.parseKafkaLocationPath(queryExloadParam.getLocationPath());
        request.setTopic(kafkaTopicUri.getTopic());
        request.setZookeeperHost(kafkaTopicUri.getHost());
        request.setZookeeperPort(kafkaTopicUri.getPort());
        return new MpprRequestContext(request);
    }
}
