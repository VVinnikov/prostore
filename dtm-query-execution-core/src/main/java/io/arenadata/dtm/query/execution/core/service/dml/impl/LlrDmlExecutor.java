package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.service.DeltaQueryPreprocessor;
import io.arenadata.dtm.query.execution.core.service.datasource.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.dml.*;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsService;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.plugin.api.dml.DmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.dml.DmlExecutor;
import io.vertx.core.Future;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class LlrDmlExecutor implements DmlExecutor<QueryResult> {

    private final DataSourcePluginService dataSourcePluginService;
    private final TargetDatabaseDefinitionService targetDatabaseDefinitionService;
    private final DeltaQueryPreprocessor deltaQueryPreprocessor;
    private final LogicViewReplacer logicViewReplacer;
    private final ColumnMetadataService columnMetadataService;
    private final InformationSchemaExecutor informationSchemaExecutor;
    private final InformationSchemaDefinitionService informationSchemaDefinitionService;
    private final LogicalSchemaProvider logicalSchemaProvider;
    private final MetricsService<RequestMetrics> metricsService;

    @Autowired
    public LlrDmlExecutor(DataSourcePluginService dataSourcePluginService,
                          TargetDatabaseDefinitionService targetDatabaseDefinitionService,
                          DeltaQueryPreprocessor deltaQueryPreprocessor,
                          LogicViewReplacer logicViewReplacer,
                          ColumnMetadataService columnMetadataService,
                          InformationSchemaExecutor informationSchemaExecutor,
                          InformationSchemaDefinitionService informationSchemaDefinitionService,
                          LogicalSchemaProvider logicalSchemaProvider,
                          MetricsService<RequestMetrics> metricsService) {
        this.dataSourcePluginService = dataSourcePluginService;
        this.targetDatabaseDefinitionService = targetDatabaseDefinitionService;
        this.deltaQueryPreprocessor = deltaQueryPreprocessor;
        this.logicViewReplacer = logicViewReplacer;
        this.informationSchemaExecutor = informationSchemaExecutor;
        this.columnMetadataService = columnMetadataService;
        this.informationSchemaDefinitionService = informationSchemaDefinitionService;
        this.logicalSchemaProvider = logicalSchemaProvider;
        this.metricsService = metricsService;
    }

    @Override
    public Future<QueryResult> execute(DmlRequestContext context) {
        val queryRequest = context.getRequest().getQueryRequest();
        val sourceRequest = new QuerySourceRequest(queryRequest, queryRequest.getSourceType());
        return logicViewReplacer.replace(sourceRequest.getQueryRequest().getSql(),
                sourceRequest.getQueryRequest().getDatamartMnemonic())
                .map(sqlWithoutViews -> {
                    QueryRequest withoutViewsRequest = sourceRequest.getQueryRequest().copy();
                    withoutViewsRequest.setSql(sqlWithoutViews);
                    return withoutViewsRequest;
                })
                .compose(deltaQueryPreprocessor::process)
                .map(request -> {
                    sourceRequest.setQueryRequest(request);
                    return request;
                })
                .compose(v -> initLogicalSchema(sourceRequest))
                .compose(this::initColumnMetaData)
                .compose(request -> executeRequest(request, context));
    }

    private Future<QuerySourceRequest> initLogicalSchema(QuerySourceRequest sourceRequest) {
        return logicalSchemaProvider.getSchema(sourceRequest.getQueryRequest())
                .map(logicalSchema -> {
                    sourceRequest.setLogicalSchema(logicalSchema);
                    return sourceRequest;
                });
    }

    private Future<QuerySourceRequest> initColumnMetaData(QuerySourceRequest request) {
        val parserRequest = new QueryParserRequest(request.getQueryRequest(), request.getLogicalSchema());
        return columnMetadataService.getColumnMetadata(parserRequest)
                .map(metadata -> {
                    request.setMetadata(metadata);
                    return request;
                });
    }

    private Future<QueryResult> executeRequest(QuerySourceRequest sourceRequest,
                                               DmlRequestContext context) {
        return Future.future(promise -> {
            if (informationSchemaDefinitionService.isInformationSchemaRequest(sourceRequest)) {
                metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                        SqlProcessingType.LLR,
                        context.getMetrics())
                        .compose(v -> informationSchemaExecute(sourceRequest))
                        .onComplete(metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                                SqlProcessingType.LLR,
                                context.getMetrics(),
                                promise));
            } else {
                targetDatabaseDefinitionService.getTargetSource(sourceRequest)
                        .compose(querySourceRequest -> pluginExecute(querySourceRequest, context.getMetrics()))
                        .onComplete(promise);
            }
        });
    }

    private Future<QueryResult> informationSchemaExecute(QuerySourceRequest querySourceRequest) {
        return Future.future(p -> informationSchemaExecutor.execute(querySourceRequest));
    }

    @SneakyThrows
    private Future<QueryResult> pluginExecute(QuerySourceRequest request, RequestMetrics requestMetrics) {
        return dataSourcePluginService.llr(
                request.getQueryRequest().getSourceType(),
                new LlrRequestContext(
                        requestMetrics,
                        new LlrRequest(
                                request.getQueryRequest(),
                                request.getLogicalSchema(),
                                request.getMetadata())
                ));
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.SELECT;
    }

}
