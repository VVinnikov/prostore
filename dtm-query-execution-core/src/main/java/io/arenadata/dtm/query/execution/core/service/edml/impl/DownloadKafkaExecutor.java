package io.arenadata.dtm.query.execution.core.service.edml.impl;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.configuration.properties.EdmlProperties;
import io.arenadata.dtm.query.execution.core.factory.MpprKafkaRequestFactory;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.dml.ColumnMetadataService;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlDownloadExecutor;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class DownloadKafkaExecutor implements EdmlDownloadExecutor {

    private final MpprKafkaRequestFactory mpprKafkaRequestFactory;
    private final ColumnMetadataService columnMetadataService;
    private final DataSourcePluginService pluginService;
    private final EdmlProperties edmlProperties;

    @Autowired
    public DownloadKafkaExecutor(DataSourcePluginService pluginService,
                                 MpprKafkaRequestFactory mpprKafkaRequestFactory,
                                 ColumnMetadataService columnMetadataService,
                                 EdmlProperties edmlProperties) {
        this.pluginService = pluginService;
        this.mpprKafkaRequestFactory = mpprKafkaRequestFactory;
        this.columnMetadataService = columnMetadataService;
        this.edmlProperties = edmlProperties;
    }

    @Override
    public void execute(EdmlRequestContext context, Handler<AsyncResult<QueryResult>> resultHandler) {
        execute(context).onComplete(resultHandler);
    }

    private Future<QueryResult> execute(EdmlRequestContext context) {
        if (context.getSourceEntity().getDestination().contains(edmlProperties.getSourceType())) {
            return mpprKafkaRequestFactory.create(context)
                    .compose(mpprRequestContext -> initColumnMetadata(context, mpprRequestContext))
                    .compose(this::executeMppr);
        } else {
            return Future.failedFuture(new IllegalStateException(
                    String.format("Source not exist in [%s]", edmlProperties.getSourceType())));
        }
    }

    private Future<MpprRequestContext> initColumnMetadata(EdmlRequestContext context,
                                                          MpprRequestContext mpprRequestContext) {
        return Future.future((Promise<MpprRequestContext> promise) -> {
            val parserRequest = new QueryParserRequest(context.getRequest().getQueryRequest(), context.getLogicalSchema());
            columnMetadataService.getColumnMetadata(parserRequest, ar -> {
                if (ar.succeeded()) {
                    mpprRequestContext.getRequest().setMetadata(ar.result());
                    promise.complete(mpprRequestContext);
                } else {
                    promise.fail(ar.cause());
                }
            });
        });
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
