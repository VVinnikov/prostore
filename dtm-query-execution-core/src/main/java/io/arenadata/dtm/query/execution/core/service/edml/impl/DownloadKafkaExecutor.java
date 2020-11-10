package io.arenadata.dtm.query.execution.core.service.edml.impl;

import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.configuration.properties.EdmlProperties;
import io.arenadata.dtm.query.execution.core.factory.MpprKafkaRequestFactory;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlDownloadExecutor;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class DownloadKafkaExecutor implements EdmlDownloadExecutor {

    private final MpprKafkaRequestFactory mpprKafkaRequestFactory;
    private final DataSourcePluginService pluginService;
    private final EdmlProperties edmlProperties;

    @Autowired
    public DownloadKafkaExecutor(DataSourcePluginService pluginService,
                                 MpprKafkaRequestFactory mpprKafkaRequestFactory,
                                 EdmlProperties edmlProperties) {
        this.pluginService = pluginService;
        this.mpprKafkaRequestFactory = mpprKafkaRequestFactory;
        this.edmlProperties = edmlProperties;
    }

    @Override
    public void execute(EdmlRequestContext context, Handler<AsyncResult<QueryResult>> resultHandler) {
        execute(context).onComplete(resultHandler);
    }

    private Future<QueryResult> execute(EdmlRequestContext context) {
        if (context.getSourceEntity().getDestination().contains(edmlProperties.getSourceType())) {
            return mpprKafkaRequestFactory.create(context)
                    .compose(this::executeMppr);
        } else {
            return Future.failedFuture(new IllegalStateException(
                    String.format("Source not exist in [%s]", edmlProperties.getSourceType())));
        }
    }

    private Future<QueryResult> executeMppr(MpprRequestContext mpprRequestContext) {
        return Future.future(promise -> pluginService.mppr(edmlProperties.getSourceType(),
                mpprRequestContext, promise));
    }

    @Override
    public ExternalTableLocationType getDownloadType() {
        return ExternalTableLocationType.KAFKA;
    }
}
