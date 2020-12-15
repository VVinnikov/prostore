package io.arenadata.dtm.query.execution.core.service.edml.impl;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.configuration.properties.EdmlProperties;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.execution.core.factory.MpprKafkaRequestFactory;
import io.arenadata.dtm.query.execution.core.service.CheckColumnTypesService;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.dml.ColumnMetadataService;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlDownloadExecutor;
import io.arenadata.dtm.query.execution.core.service.impl.CheckColumnTypesServiceImpl;
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
    private final CheckColumnTypesService checkColumnTypesService;

    @Autowired
    public DownloadKafkaExecutor(DataSourcePluginService pluginService,
                                 MpprKafkaRequestFactory mpprKafkaRequestFactory,
                                 EdmlProperties edmlProperties,
                                 CheckColumnTypesService checkColumnTypesService,
                                 ColumnMetadataService columnMetadataService) {
        this.pluginService = pluginService;
        this.mpprKafkaRequestFactory = mpprKafkaRequestFactory;
        this.columnMetadataService = columnMetadataService;
        this.edmlProperties = edmlProperties;
        this.checkColumnTypesService = checkColumnTypesService;
    }

    @Override
    public void execute(EdmlRequestContext context, AsyncHandler<QueryResult> handler) {
        execute(context).onComplete(handler);
    }

    private Future<QueryResult> execute(EdmlRequestContext context) {
        if (checkDestinationType(context)) {
            QueryParserRequest queryParserRequest = new QueryParserRequest(context.getRequest().getQueryRequest(),
                    context.getLogicalSchema());
            //TODO add checking for column names, and throw new ColumnNotExistsException if will be error
            return checkColumnTypesService.check(context.getDestinationEntity().getFields(), queryParserRequest)
                    .compose(areEqual -> areEqual ? mpprKafkaRequestFactory.create(context)
                            : Future.failedFuture(new DtmException(String.format(CheckColumnTypesServiceImpl.FAIL_CHECK_COLUMNS_PATTERN,
                            context.getDestinationEntity().getName()))))
                    .compose(mpprRequestContext -> initColumnMetadata(context, mpprRequestContext))
                    .compose(this::executeMppr);
        } else {
            return Future.failedFuture(new DtmException(
                    String.format("Source not exist in [%s]", edmlProperties.getSourceType())));
        }
    }

    private boolean checkDestinationType(EdmlRequestContext context) {
        return context.getLogicalSchema().stream()
            .flatMap(datamart -> datamart.getEntities().stream())
            .allMatch(entity -> entity.getDestination().contains(edmlProperties.getSourceType()));
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
                mpprRequestContext, (AsyncHandler<QueryResult>) promise));
    }

    @Override
    public ExternalTableLocationType getDownloadType() {
        return ExternalTableLocationType.KAFKA;
    }
}
