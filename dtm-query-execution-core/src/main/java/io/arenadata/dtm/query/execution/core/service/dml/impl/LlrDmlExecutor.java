package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.SourceQueryTemplateValue;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.dml.DmlType;
import io.arenadata.dtm.query.calcite.core.service.DeltaQueryPreprocessor;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import io.arenadata.dtm.query.calcite.core.util.SqlNodeUtil;
import io.arenadata.dtm.query.execution.core.service.datasource.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.dml.*;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsService;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.dml.DmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.dml.DmlExecutor;
import io.vertx.core.Future;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;

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
    private final CacheService<QueryTemplateKey, SourceQueryTemplateValue> queryCacheService;

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
                          @Qualifier("coreQueryTemplateCacheService") CacheService<QueryTemplateKey, SourceQueryTemplateValue> queryCacheService) {
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
        return logicViewReplacer.replace(context.getQuery(),
                sourceRequest.getQueryRequest().getDatamartMnemonic())
                .map(sqlWithoutViews -> {
                    QueryRequest withoutViewsRequest = sourceRequest.getQueryRequest();
                    withoutViewsRequest.setSql(sqlWithoutViews.toString());
                    withoutViewsRequest.setSqlNode(sqlWithoutViews);
                    context.setQuery(sqlWithoutViews);
                    return withoutViewsRequest;
                })
                .compose(v -> getRequestFromCacheOrInit(sourceRequest, context))
                .compose(request -> executeRequest(request, context));
    }

    private Future<QuerySourceRequest> getRequestFromCacheOrInit(QuerySourceRequest sourceRequest,
                                                                 DmlRequestContext context) {
        return Future.future(promise -> {
            val copySqlNode = SqlNodeUtil.copy(context.getQuery());
            val templateResult = templateExtractor.extract(copySqlNode);
            val queryTemplateValue = queryCacheService.get(QueryTemplateKey.builder()
                    .sourceQueryTemplate(templateResult.getTemplate())
                    .build());
            sourceRequest.setQueryTemplate(templateResult);
            if (queryTemplateValue != null) {
                sourceRequest.setQueryTemplate(templateResult);
                sourceRequest.setMetadata(queryTemplateValue.getMetadata());
                sourceRequest.setLogicalSchema(queryTemplateValue.getLogicalSchema());
                sourceRequest.getQueryRequest().setDeltaInformations(queryTemplateValue.getDeltaInformations());
                sourceRequest.getQueryRequest().setSql(queryTemplateValue.getSql());
                promise.complete(sourceRequest);
            } else {
                initRequestAttributes(sourceRequest, context)
                        .compose(request ->
                                queryCacheService.put(QueryTemplateKey.builder()
                                                .sourceQueryTemplate(templateResult.getTemplate())
                                                .logicalSchema(request.getLogicalSchema())
                                                .build(),
                                        SourceQueryTemplateValue.builder()
                                                .sql(request.getQueryRequest().getSql())
                                                .deltaInformations(request.getQueryRequest().getDeltaInformations())
                                                .metadata(request.getMetadata())
                                                .logicalSchema(request.getLogicalSchema())
                                                .build())
                                        .map(value -> request)
                        )
                        .onComplete(promise);
            }
        });
    }

    private Future<QuerySourceRequest> initRequestAttributes(QuerySourceRequest sourceRequest,
                                                             DmlRequestContext context) {
        return deltaQueryPreprocessor.process(context.getQuery())
                .map(preprocessorResponse -> {
                    QueryRequest queryRequest = sourceRequest.getQueryRequest();
                    queryRequest.setDeltaInformations(preprocessorResponse.getDeltaInformations());
                    context.setQuery(preprocessorResponse.getSqlNode());
                    return context;
                })
                .compose(v ->
                        getLogicalSchema(
                                context.getQuery(),
                                context.getRequest().getQueryRequest().getDatamartMnemonic()
                        ).map(schema -> {
                            sourceRequest.setLogicalSchema(schema);
                            return sourceRequest;
                        }))
                .compose(request -> initColumnMetaData(request, context.getQuery()));
    }

    private Future<List<Datamart>> getLogicalSchema(SqlNode query, String datamart) {
        return logicalSchemaProvider.getSchemaFromQuery(query, datamart);
    }

    private Future<QuerySourceRequest> initColumnMetaData(QuerySourceRequest request, SqlNode query) {
        val parserRequest = new QueryParserRequest(query, request.getLogicalSchema());
        return columnMetadataService.getColumnMetadata(parserRequest)
                .map(metadata -> {
                    request.setMetadata(metadata);
                    return request;
                });
    }

    private Future<QueryResult> executeRequest(QuerySourceRequest sourceRequest,
                                               DmlRequestContext context) {
        return Future.future(promise -> {
            sourceRequest.getQueryRequest().setSqlNode(context.getQuery());
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
                        .compose(querySourceRequest -> pluginExecute(querySourceRequest,
                                context.getMetrics(),
                                context.getQuery()))
                        .onComplete(promise);
            }
        });
    }

    private Future<QueryResult> informationSchemaExecute(QuerySourceRequest querySourceRequest) {
        return informationSchemaExecutor.execute(querySourceRequest);
    }

    @SneakyThrows
    private Future<QueryResult> pluginExecute(QuerySourceRequest request,
                                              RequestMetrics requestMetrics,
                                              SqlNode query) {
        //FIXME after checking performance
        final LlrRequest llrRequest = new LlrRequest(
                request.getQueryTemplate(),
                request.getQueryRequest(),
                request.getLogicalSchema(),
                request.getMetadata(),
                query);
        return dataSourcePluginService.llr(
                request.getQueryRequest().getSourceType(),
                new LlrRequestContext(
                        requestMetrics,
                        llrRequest
                ));
    }

    @Override
    public DmlType getType() {
        return DmlType.LLR;
    }

}
