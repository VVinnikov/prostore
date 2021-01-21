package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.SourceQueryTemplateValue;
import io.arenadata.dtm.common.delta.DeltaInformation;
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
import io.arenadata.dtm.query.execution.core.dto.dml.DmlRequestContext;
import io.arenadata.dtm.query.execution.core.dto.dml.LlrRequestContext;
import io.arenadata.dtm.query.execution.core.service.datasource.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.dml.*;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsService;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
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
        val sourceRequest = new QuerySourceRequest(queryRequest, context.getSqlNode(), context.getSourceType());
        LlrRequestContext llrContext = LlrRequestContext.builder()
                .sourceRequest(sourceRequest)
                .dmlRequestContext(context)
                .build();
        return logicViewReplacer.replace(context.getSqlNode(),
                sourceRequest.getQueryRequest().getDatamartMnemonic())
                .map(sqlWithoutViews -> {
                    QueryRequest withoutViewsRequest = sourceRequest.getQueryRequest();
                    withoutViewsRequest.setSql(sqlWithoutViews.toString());
                    context.setSqlNode(sqlWithoutViews);
                    return withoutViewsRequest;
                })
                .compose(v -> getRequestFromCacheOrInit(llrContext))
                .compose(v -> executeRequest(llrContext));
    }

    private Future<LlrRequestContext> getRequestFromCacheOrInit(LlrRequestContext llrContext) {
        return Future.future(promise -> {
            val copySqlNode = SqlNodeUtil.copy(llrContext.getDmlRequestContext().getSqlNode());
            val templateResult = templateExtractor.extract(copySqlNode);
            val queryTemplateValue = queryCacheService.get(QueryTemplateKey.builder()
                    .sourceQueryTemplate(templateResult.getTemplate())
                    .build());
            QuerySourceRequest sourceRequest = llrContext.getSourceRequest();
            sourceRequest.setQueryTemplate(templateResult);
            if (queryTemplateValue != null) {
                sourceRequest.setQueryTemplate(templateResult);
                sourceRequest.setMetadata(queryTemplateValue.getMetadata());
                sourceRequest.setLogicalSchema(queryTemplateValue.getLogicalSchema());
                sourceRequest.getQueryRequest().setSql(queryTemplateValue.getSql());
                llrContext.setDeltaInformations(queryTemplateValue.getDeltaInformations());
                promise.complete();
            } else {
                initRequestAttributes(llrContext)
                        .compose(v ->
                                queryCacheService.put(QueryTemplateKey.builder()
                                                .sourceQueryTemplate(templateResult.getTemplate())
                                                .logicalSchema(sourceRequest.getLogicalSchema())
                                                .build(),
                                        SourceQueryTemplateValue.builder()
                                                .deltaInformations(llrContext.getDeltaInformations())
                                                .logicalSchema(sourceRequest.getLogicalSchema())
                                                .sql(sourceRequest.getQueryRequest().getSql())
                                                .metadata(sourceRequest.getMetadata())
                                                .build())
                                        .map(value -> llrContext)
                        )
                        .onComplete(promise);
            }
        });
    }

    private Future<LlrRequestContext> initRequestAttributes(LlrRequestContext llrContext) {
        return deltaQueryPreprocessor.process(llrContext.getDmlRequestContext().getSqlNode())
                .map(preprocessorResponse -> {
                    QueryRequest queryRequest = llrContext.getSourceRequest().getQueryRequest();
                    llrContext.setDeltaInformations(preprocessorResponse.getDeltaInformations());
                    llrContext.getDmlRequestContext().setSqlNode(preprocessorResponse.getSqlNode());
                    return llrContext;
                })
                .compose(v ->
                        getLogicalSchema(
                                llrContext.getDmlRequestContext().getSqlNode(),
                                llrContext.getDmlRequestContext().getRequest().getQueryRequest().getDatamartMnemonic()
                        ).map(schema -> {
                            llrContext.getSourceRequest().setLogicalSchema(schema);
                            return llrContext;
                        }))
                .compose(v -> initColumnMetaData(llrContext));
    }

    private Future<List<Datamart>> getLogicalSchema(SqlNode query, String datamart) {
        return logicalSchemaProvider.getSchemaFromQuery(query, datamart);
    }

    private Future<LlrRequestContext> initColumnMetaData(LlrRequestContext llrContext) {
        SqlNode query = llrContext.getDmlRequestContext().getSqlNode();
        val parserRequest = new QueryParserRequest(query, llrContext.getSourceRequest().getLogicalSchema());
        return columnMetadataService.getColumnMetadata(parserRequest)
                .map(metadata -> {
                    llrContext.getSourceRequest().setMetadata(metadata);
                    return llrContext;
                });
    }

    private Future<QueryResult> executeRequest(LlrRequestContext llrContext) {
        return Future.future(promise -> {
            val sourceRequest = llrContext.getSourceRequest();
            val dmlRequestContext = llrContext.getDmlRequestContext();
            if (informationSchemaDefinitionService.isInformationSchemaRequest(llrContext.getDeltaInformations())) {
                metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                        SqlProcessingType.LLR,
                        dmlRequestContext.getMetrics())
                        .compose(v -> informationSchemaExecute(sourceRequest))
                        .onComplete(metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                                SqlProcessingType.LLR,
                                dmlRequestContext.getMetrics(),
                                promise));
            } else {
                targetDatabaseDefinitionService.getTargetSource(sourceRequest, dmlRequestContext.getSqlNode())
                        .compose(querySourceRequest -> pluginExecute(querySourceRequest,
                                dmlRequestContext,
                                llrContext.getDeltaInformations()))
                        .onComplete(promise);
            }
        });
    }

    private Future<QueryResult> informationSchemaExecute(QuerySourceRequest querySourceRequest) {
        return informationSchemaExecutor.execute(querySourceRequest);
    }

    @SneakyThrows
    private Future<QueryResult> pluginExecute(QuerySourceRequest sourceRequest,
                                              DmlRequestContext context,
                                              List<DeltaInformation> deltaInformations) {

        QueryRequest queryRequest = context.getRequest().getQueryRequest();
        return dataSourcePluginService.llr(sourceRequest.getSourceType(),
                context.getMetrics(),
                LlrRequest.builder()
                        .sourceQueryTemplateResult(sourceRequest.getQueryTemplate())
                        .datamartMnemonic(queryRequest.getDatamartMnemonic())
                        .schema(sourceRequest.getLogicalSchema())
                        .requestId(queryRequest.getRequestId())
                        .metadata(sourceRequest.getMetadata())
                        .deltaInformations(deltaInformations)
                        .sqlNode(context.getSqlNode())
                        .envName(context.getEnvName())
                        .build());
    }

    @Override
    public DmlType getType() {
        return DmlType.LLR;
    }

}
