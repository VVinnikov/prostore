package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.*;
import io.arenadata.dtm.query.calcite.core.extension.dml.DmlType;
import io.arenadata.dtm.query.calcite.core.service.DeltaQueryPreprocessor;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import io.arenadata.dtm.query.execution.core.dto.cache.QueryTemplateKey;
import io.arenadata.dtm.query.execution.core.dto.cache.QueryTemplateValue;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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
    private final QueryTemplateExtractor templateExtractor;
    private final CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService;

    @Autowired
    public LlrDmlExecutor(DataSourcePluginService dataSourcePluginService,
                          TargetDatabaseDefinitionService targetDatabaseDefinitionService,
                          DeltaQueryPreprocessor deltaQueryPreprocessor,
                          LogicViewReplacer logicViewReplacer,
                          ColumnMetadataService columnMetadataService,
                          InformationSchemaExecutor informationSchemaExecutor,
                          InformationSchemaDefinitionService informationSchemaDefinitionService,
                          LogicalSchemaProvider logicalSchemaProvider,
                          MetricsService<RequestMetrics> metricsService,
                          @Qualifier("coreQueryTmplateExtractor") QueryTemplateExtractor templateExtractor,
                          @Qualifier("coreQueryTemplateCacheService") CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService) {
        this.dataSourcePluginService = dataSourcePluginService;
        this.targetDatabaseDefinitionService = targetDatabaseDefinitionService;
        this.deltaQueryPreprocessor = deltaQueryPreprocessor;
        this.logicViewReplacer = logicViewReplacer;
        this.informationSchemaExecutor = informationSchemaExecutor;
        this.columnMetadataService = columnMetadataService;
        this.informationSchemaDefinitionService = informationSchemaDefinitionService;
        this.logicalSchemaProvider = logicalSchemaProvider;
        this.metricsService = metricsService;
        this.templateExtractor = templateExtractor;
        this.queryCacheService = queryCacheService;
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
                .compose(withoutWiqRequest -> getRequestFromCacheOrInit(sourceRequest, withoutWiqRequest))
                .compose(request -> executeRequest(request, context));
    }

    private Future<QuerySourceRequest> getRequestFromCacheOrInit(QuerySourceRequest sourceRequest,
                                                                 QueryRequest withoutViewsRequest) {
        return Future.future(promise -> {
            final QueryTemplateResult templateResult = templateExtractor.extract(withoutViewsRequest.getSql());
            final QueryTemplateValue queryTemplateValue = queryCacheService.get(QueryTemplateKey.builder()
                    .queryTemplate(templateResult.getTemplate())
                    .build());
            sourceRequest.setQueryTemplate(templateResult);
            if (queryTemplateValue != null) {
                sourceRequest.setQueryTemplate(templateResult);
                sourceRequest.setMetadata(queryTemplateValue.getMetadata());
                sourceRequest.setLogicalSchema(queryTemplateValue.getLogicalSchema());
                sourceRequest.getQueryRequest().setDeltaInformations(queryTemplateValue.getDeltaInformations());
                promise.complete(sourceRequest);
            } else {
                initRequestAttributes(withoutViewsRequest, sourceRequest)
                        .compose(request ->
                            queryCacheService.put(QueryTemplateKey.builder()
                                    .queryTemplate(templateResult.getTemplate())
                                    .logicalSchema(sourceRequest.getLogicalSchema())
                                    .build(),
                                    QueryTemplateValue.builder()
                                            .sql(templateResult.getTemplate())
                                            .deltaInformations(sourceRequest.getQueryRequest().getDeltaInformations())
                                            .metadata(sourceRequest.getMetadata())
                                            .logicalSchema(sourceRequest.getLogicalSchema())
                                            .build())
                                .map(value -> request)
                        )
                        .onComplete(promise);
            }
        });
    }

    private Future<QuerySourceRequest> initRequestAttributes(QueryRequest withoutViewsRequest,
                                                             QuerySourceRequest sourceRequest) {
        return deltaQueryPreprocessor.process(withoutViewsRequest)
                .map(request -> {
                    sourceRequest.setQueryRequest(request);
                    return request;
                })
                .compose(v -> initLogicalSchema(sourceRequest))
                .compose(this::initColumnMetaData);
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
        return informationSchemaExecutor.execute(querySourceRequest);
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
    public DmlType getType() {
        return DmlType.LLR;
    }

}
