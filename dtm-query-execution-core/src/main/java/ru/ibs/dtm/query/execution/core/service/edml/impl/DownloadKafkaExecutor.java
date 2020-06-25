package ru.ibs.dtm.query.execution.core.service.edml.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.plugin.exload.Type;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.core.configuration.properties.EdmlProperties;
import ru.ibs.dtm.query.execution.core.factory.MpprKafkaRequestFactory;
import ru.ibs.dtm.query.execution.core.service.DataSourcePluginService;
import ru.ibs.dtm.query.execution.core.service.edml.EdmlDownloadExecutor;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;

@Service
@Slf4j
public class DownloadKafkaExecutor implements EdmlDownloadExecutor {

    private final DataSourcePluginService pluginService;
    private final MpprKafkaRequestFactory mpprKafkaRequestFactory;
    private final EdmlProperties edmlProperties;

    public DownloadKafkaExecutor(DataSourcePluginService pluginService, MpprKafkaRequestFactory mpprKafkaRequestFactory, EdmlProperties edmlProperties) {
        this.pluginService = pluginService;
        this.mpprKafkaRequestFactory = mpprKafkaRequestFactory;
        this.edmlProperties = edmlProperties;
    }

    @Override
    public void execute(EdmlRequestContext context, Handler<AsyncResult<QueryResult>> resultHandler) {
        pluginService.mpprKafka(edmlProperties.getSourceType(), mpprKafkaRequestFactory.create(context.getRequest().getQueryRequest(),
                context.getExloadParam(), context.getSchema()), resultHandler);
    }

    @Override
    public Type getDownloadType() {
        return Type.KAFKA_TOPIC;
    }
}
