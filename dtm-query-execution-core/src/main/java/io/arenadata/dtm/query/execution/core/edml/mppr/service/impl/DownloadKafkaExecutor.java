package io.arenadata.dtm.query.execution.core.edml.mppr.service.impl;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.edml.configuration.EdmlProperties;
import io.arenadata.dtm.query.execution.core.edml.mppr.factory.MpprKafkaRequestFactory;
import io.arenadata.dtm.query.execution.core.edml.mppr.service.EdmlDownloadExecutor;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.dml.service.ColumnMetadataService;
import io.arenadata.dtm.query.execution.core.base.service.column.CheckColumnTypesService;
import io.arenadata.dtm.query.execution.core.base.service.column.CheckColumnTypesServiceImpl;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequest;
import io.vertx.core.Future;
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
    public Future<QueryResult> execute(EdmlRequestContext context) {
        return executeInternal(context);
    }

    private Future<QueryResult> executeInternal(EdmlRequestContext context) {
        if (checkDestinationType(context)) {
            val queryParserRequest = new QueryParserRequest(context.getDmlSubQuery(), context.getLogicalSchema());
            //TODO add checking for column names, and throw new ColumnNotExistsException if will be error
            return checkColumnTypesService.check(context.getDestinationEntity().getFields(), queryParserRequest)
                    .compose(areEqual -> areEqual ? mpprKafkaRequestFactory.create(context)
                            : Future.failedFuture(getFailCheckColumnsException(context)))
                    .compose(mpprKafkaRequest -> initColumnMetadata(context, mpprKafkaRequest))
                    .compose(mpprKafkaRequest ->
                            pluginService.mppr(edmlProperties.getSourceType(), context.getMetrics(), mpprKafkaRequest));
        } else {
            return Future.failedFuture(new DtmException(
                    String.format("Queried entity is missing for the specified DATASOURCE_TYPE %s", edmlProperties.getSourceType())));
        }
    }

    private DtmException getFailCheckColumnsException(EdmlRequestContext context) {
        return new DtmException(String.format(CheckColumnTypesServiceImpl.FAIL_CHECK_COLUMNS_PATTERN,
                context.getDestinationEntity().getName()));
    }

    private boolean checkDestinationType(EdmlRequestContext context) {
        return context.getLogicalSchema().stream()
                .flatMap(datamart -> datamart.getEntities().stream())
                .allMatch(entity -> entity.getDestination().contains(edmlProperties.getSourceType()));
    }

    private Future<MpprRequest> initColumnMetadata(EdmlRequestContext context,
                                                   MpprRequest mpprRequest) {
        val parserRequest = new QueryParserRequest(context.getDmlSubQuery(), context.getLogicalSchema());
        return columnMetadataService.getColumnMetadata(parserRequest)
                .map(metadata -> {
                    mpprRequest.setMetadata(metadata);
                    return mpprRequest;
                });
    }

    @Override
    public ExternalTableLocationType getDownloadType() {
        return ExternalTableLocationType.KAFKA;
    }
}
