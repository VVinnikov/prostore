package ru.ibs.dtm.query.execution.core.service.edml.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.plugin.exload.Type;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.calcite.core.service.DeltaQueryPreprocessor;
import ru.ibs.dtm.query.execution.core.configuration.properties.EdmlProperties;
import ru.ibs.dtm.query.execution.core.factory.MpprKafkaRequestFactory;
import ru.ibs.dtm.query.execution.core.service.DataSourcePluginService;
import ru.ibs.dtm.query.execution.core.service.edml.EdmlDownloadExecutor;
import ru.ibs.dtm.query.execution.plugin.api.edml.EdmlRequestContext;

@Service
@Slf4j
public class DownloadKafkaExecutor implements EdmlDownloadExecutor {

    private final MpprKafkaRequestFactory mpprKafkaRequestFactory;
    private final DeltaQueryPreprocessor deltaQueryPreprocessor;
    private final DataSourcePluginService pluginService;
    private final EdmlProperties edmlProperties;

    public DownloadKafkaExecutor(DataSourcePluginService pluginService,
                                 MpprKafkaRequestFactory mpprKafkaRequestFactory,
                                 DeltaQueryPreprocessor deltaQueryPreprocessor,
                                 EdmlProperties edmlProperties) {
        this.pluginService = pluginService;
        this.mpprKafkaRequestFactory = mpprKafkaRequestFactory;
        this.deltaQueryPreprocessor = deltaQueryPreprocessor;
        this.edmlProperties = edmlProperties;
    }

    @Override
    public void execute(EdmlRequestContext context, Handler<AsyncResult<QueryResult>> resultHandler) {
        deltaQueryPreprocessor.process(context.getRequest().getQueryRequest())
                .compose(qRequest -> getMpprFuture(context, qRequest))
                .onComplete(resultHandler);

    }

    private Future<QueryResult> getMpprFuture(EdmlRequestContext context, QueryRequest queryRequest) {
        return Future.future(p -> {
            val mpprRequestContext = mpprKafkaRequestFactory.create(queryRequest,
                    context.getExloadParam(),
                    context.getLogicalSchema());
            pluginService.mpprKafka(edmlProperties.getSourceType(), mpprRequestContext, p);
        });
    }

    @Override
    public Type getDownloadType() {
        return Type.KAFKA_TOPIC;
    }
}
