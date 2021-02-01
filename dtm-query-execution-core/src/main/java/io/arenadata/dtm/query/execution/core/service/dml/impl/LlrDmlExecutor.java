package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.PreparedQueryKey;
import io.arenadata.dtm.common.cache.PreparedQueryValue;
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
import io.arenadata.dtm.query.execution.core.dto.dml.DmlRequestContext;
import io.arenadata.dtm.query.execution.core.dto.dml.LlrRequestContext;
import io.arenadata.dtm.query.execution.core.exception.query.QueriedEntityIsMissingException;
import io.arenadata.dtm.query.execution.core.service.datasource.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.dml.*;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsService;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.vertx.core.Future;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.Set;

@Component
public class LlrDmlExecutor implements DmlExecutor<QueryResult> {
    //TODO need to refactor this class if it's possible
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
    private final CacheService<PreparedQueryKey, PreparedQueryValue> preparedQueryCacheService;

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
                          @Qualifier("coreQueryTemplateCacheService") CacheService<QueryTemplateKey, SourceQueryTemplateValue> queryCacheService,
                          @Qualifier("corePreparedQueryCacheService") CacheService<PreparedQueryKey, PreparedQueryValue> preparedQueryCacheService) {
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
        this.preparedQueryCacheService = preparedQueryCacheService;
    }

    @Override
    public Future<QueryResult> execute(DmlRequestContext context) {
        val queryRequest = context.getRequest().getQueryRequest();
        val sourceRequest = new QuerySourceRequest(queryRequest, context.getSqlNode(), context.getSourceType());
        LlrRequestContext llrContext = LlrRequestContext.builder()
                .originalQuery(context.getSqlNode())
                .sourceRequest(sourceRequest)
                .dmlRequestContext(context)
                .build();
        if (queryRequest.isPrepare()) {
            return prepareQuery(llrContext);
        } else {
            return executeQuery(llrContext);
        }
    }

    private Future<QueryResult> prepareQuery(LlrRequestContext llrContext) {
        return Future.future(promise -> {
            preparedQueryCacheService.put(new PreparedQueryKey(llrContext.getSourceRequest().getQueryRequest().getSql()),
                    new PreparedQueryValue(llrContext.getDmlRequestContext().getSqlNode()));
            replaceViewFromSqlNode(llrContext)
                    .compose(v -> getQueryTemplateValueFromCacheOrCreate(llrContext))
                    .compose(queryTemplateValue -> getQuerySourceTypeAndUpdateQueryCacheIfNeeded(llrContext, queryTemplateValue))
                    .compose(sourceType -> dataSourcePluginService.prepareLlr(sourceType,
                            llrContext.getDmlRequestContext().getMetrics(),
                            createLlrRequest(llrContext)))
                    .onSuccess(result -> promise.complete(QueryResult.emptyResult()))
                    .onFailure(promise::fail);
        });
    }

    private Future<SourceQueryTemplateValue> getQueryTemplateValueFromCacheOrCreate(LlrRequestContext llrContext) {
        val queryTemplateValueOpt = getQueryTemplateValue(llrContext);
        if (queryTemplateValueOpt.isPresent()) {
            val queryTemplateValue = queryTemplateValueOpt.get();
            initLlrContextFromCache(llrContext, queryTemplateValue);
            return Future.succeededFuture(queryTemplateValue);
        } else {
            val newQueryTemplateKey = QueryTemplateKey.builder().build();
            val newQueryTemplateValue = SourceQueryTemplateValue.builder().build();
            return initDeltaInformations(llrContext)
                    .compose(v -> initLlrContext(llrContext))
                    .compose(v -> targetDatabaseDefinitionService.getAcceptableSourceTypes(llrContext.getSourceRequest()))
                    .map(sourceTypes -> {
                        initQueryTemplateObjects(llrContext, newQueryTemplateKey, newQueryTemplateValue, sourceTypes);
                        return sourceTypes;
                    })
                    .compose(v -> queryCacheService.put(QueryTemplateKey.builder()
                                    .sourceQueryTemplate(llrContext.getSourceRequest().getQueryTemplate().getTemplate())
                                    .logicalSchema(llrContext.getSourceRequest().getLogicalSchema())
                                    .build(),
                            newQueryTemplateValue));
        }
    }

    private Future<QueryResult> executeQuery(LlrRequestContext llrContext) {
        return replaceViewFromSqlNode(llrContext)
                .map(v -> getQueryTemplateValue(llrContext))
                .compose(queryTemplateValue -> execute(queryTemplateValue, llrContext));
    }

    private Future<QueryRequest> replaceViewFromSqlNode(LlrRequestContext llrContext) {
        return logicViewReplacer.replace(llrContext.getDmlRequestContext().getSqlNode(),
                llrContext.getSourceRequest().getQueryRequest().getDatamartMnemonic())
                .map(sqlWithoutViews -> {
                    QueryRequest withoutViewsRequest = llrContext.getSourceRequest().getQueryRequest();
                    withoutViewsRequest.setSql(sqlWithoutViews.toString());
                    llrContext.getDmlRequestContext().setSqlNode(sqlWithoutViews);
                    return withoutViewsRequest;
                });
    }

    private Optional<SourceQueryTemplateValue> getQueryTemplateValue(LlrRequestContext context) {
        val copySqlNode = SqlNodeUtil.copy(context.getDmlRequestContext().getSqlNode());
        val templateResult = templateExtractor.extract(copySqlNode);
        val queryTemplateValue = queryCacheService.get(QueryTemplateKey.builder()
                .sourceQueryTemplate(templateResult.getTemplate())
                .build());
        context.getSourceRequest().setQueryTemplate(templateResult);
        return Optional.ofNullable(queryTemplateValue);
    }

    private Future<QueryResult> execute(Optional<SourceQueryTemplateValue> queryTemplateValue,
                                        LlrRequestContext llrContext) {
        return Future.future(promise -> {
            if (queryTemplateValue.isPresent()) {
                SourceQueryTemplateValue templateValue = queryTemplateValue.get();
                initLlrContextFromCache(llrContext, templateValue);
                getQuerySourceTypeAndUpdateQueryCacheIfNeeded(llrContext, templateValue)
                        .compose(sourceType -> dataSourcePluginService.llr(sourceType,
                                llrContext.getDmlRequestContext().getMetrics(),
                                createLlrRequest(llrContext)))
                        .onComplete(promise);
            } else {
                initDeltaInformations(llrContext)
                        .compose(v -> initLlrContext(llrContext))
                        .compose(v -> executeRequest(llrContext))
                        .onComplete(promise);
            }
        });
    }

    private void initLlrContextFromCache(LlrRequestContext llrContext, SourceQueryTemplateValue queryTemplateValue) {
        llrContext.getSourceRequest().setMetadata(queryTemplateValue.getMetadata());
        llrContext.getSourceRequest().setLogicalSchema(queryTemplateValue.getLogicalSchema());
        llrContext.getSourceRequest().getQueryRequest().setSql(queryTemplateValue.getSql());
        llrContext.setDeltaInformations(queryTemplateValue.getDeltaInformations());
    }

    private Future<SourceType> getQuerySourceTypeAndUpdateQueryCacheIfNeeded(LlrRequestContext llrContext,
                                                                             SourceQueryTemplateValue queryTemplateCache) {
        if (llrContext.getSourceRequest().getSourceType() == null
                && queryTemplateCache.getLeastQueryCostSourceType() == null) {
            return targetDatabaseDefinitionService.getSourceTypeWithLeastQueryCost(queryTemplateCache.getAvailableSourceTypes(),
                    llrContext.getSourceRequest())
                    .compose(leastQueryCostSourceType -> {
                                queryTemplateCache.setLeastQueryCostSourceType(leastQueryCostSourceType);
                                return queryCacheService.put(QueryTemplateKey.builder()
                                                .sourceQueryTemplate(llrContext.getSourceRequest().getQueryTemplate().getTemplate())
                                                .logicalSchema(llrContext.getSourceRequest().getLogicalSchema())
                                                .build(),
                                        queryTemplateCache);
                            }
                    )
                    .map(v -> queryTemplateCache.getLeastQueryCostSourceType());
        } else if (llrContext.getSourceRequest().getSourceType() != null
                && !queryTemplateCache.getAvailableSourceTypes().contains(llrContext.getSourceRequest().getSourceType())) {
            return Future.failedFuture(new QueriedEntityIsMissingException(llrContext.getSourceRequest().getSourceType()));
        } else {
            return Future.succeededFuture(llrContext.getSourceRequest().getSourceType() == null ?
                    queryTemplateCache.getLeastQueryCostSourceType() : llrContext.getSourceRequest().getSourceType());
        }
    }

    private Future<LlrRequestContext> initDeltaInformations(LlrRequestContext llrContext) {
        return deltaQueryPreprocessor.process(llrContext.getDmlRequestContext().getSqlNode())
                .map(preprocessorResponse -> {
                    QueryRequest queryRequest = llrContext.getSourceRequest().getQueryRequest();
                    llrContext.setDeltaInformations(preprocessorResponse.getDeltaInformations());
                    llrContext.getDmlRequestContext().setSqlNode(preprocessorResponse.getSqlNode());
                    return llrContext;
                });
    }

    private Future<QueryResult> executeRequest(LlrRequestContext llrContext) {
        return Future.future(promise -> {
            val sourceRequest = llrContext.getSourceRequest();
            val dmlRequestContext = llrContext.getDmlRequestContext();
            if (informationSchemaDefinitionService.isInformationSchemaRequest(llrContext.getDeltaInformations())) {
                //TODO check this queries
                metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                        SqlProcessingType.LLR,
                        dmlRequestContext.getMetrics())
                        .compose(v -> informationSchemaDefinitionService.checkAccessToSystemLogicalTables(llrContext.getOriginalQuery()))
                        .compose(v -> informationSchemaExecutor.execute(sourceRequest))
                        .onComplete(metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                                SqlProcessingType.LLR,
                                dmlRequestContext.getMetrics(),
                                promise));
            } else {
                val newQueryTemplateKey = QueryTemplateKey.builder().build();
                val newQueryTemplateValue = SourceQueryTemplateValue.builder().build();
                initLlrContext(llrContext)
                        .compose(v -> targetDatabaseDefinitionService.getAcceptableSourceTypes(llrContext.getSourceRequest()))
                        .map(sourceTypes -> {
                            initQueryTemplateObjects(llrContext, newQueryTemplateKey, newQueryTemplateValue, sourceTypes);
                            return sourceTypes;
                        })
                        .compose(v -> queryCacheService.put(newQueryTemplateKey, newQueryTemplateValue))
                        .compose(queryTemplateValue -> getQuerySourceTypeAndUpdateQueryCacheIfNeeded(llrContext, queryTemplateValue))
                        .compose(sourceType -> dataSourcePluginService.llr(sourceType,
                                llrContext.getDmlRequestContext().getMetrics(),
                                createLlrRequest(llrContext)))
                        .onComplete(promise);
            }
        });
    }

    private void initQueryTemplateObjects(LlrRequestContext llrContext,
                                          QueryTemplateKey newQueryTemplateKey,
                                          SourceQueryTemplateValue newQueryTemplateValue,
                                          Set<SourceType> sourceTypes) {
        newQueryTemplateKey.setSourceQueryTemplate(llrContext.getSourceRequest().getQueryTemplate().getTemplate());
        newQueryTemplateKey.setLogicalSchema(llrContext.getSourceRequest().getLogicalSchema());
        newQueryTemplateValue.setDeltaInformations(llrContext.getDeltaInformations());
        newQueryTemplateValue.setMetadata(llrContext.getSourceRequest().getMetadata());
        newQueryTemplateValue.setLogicalSchema(llrContext.getSourceRequest().getLogicalSchema());
        newQueryTemplateValue.setSql(llrContext.getSourceRequest().getQueryRequest().getSql());
        newQueryTemplateValue.setAvailableSourceTypes(sourceTypes);
    }

    private Future<LlrRequestContext> initLlrContext(LlrRequestContext llrContext) {
        return logicalSchemaProvider.getSchemaFromQuery(
                llrContext.getDmlRequestContext().getSqlNode(),
                llrContext.getDmlRequestContext().getRequest().getQueryRequest().getDatamartMnemonic())
                .map(schema -> {
                    llrContext.getSourceRequest().setLogicalSchema(schema);
                    return llrContext;
                })
                .compose(v -> columnMetadataService.getColumnMetadata(
                        new QueryParserRequest(llrContext.getDmlRequestContext().getSqlNode(),
                                llrContext.getSourceRequest().getLogicalSchema()))
                        .map(metadata -> {
                            llrContext.getSourceRequest().setMetadata(metadata);
                            return llrContext;
                        }));
    }

    private LlrRequest createLlrRequest(LlrRequestContext context) {
        QueryRequest queryRequest = context.getDmlRequestContext().getRequest().getQueryRequest();
        return LlrRequest.builder()
                .sourceQueryTemplateResult(context.getSourceRequest().getQueryTemplate())
                .datamartMnemonic(queryRequest.getDatamartMnemonic())
                .schema(context.getSourceRequest().getLogicalSchema())
                .requestId(queryRequest.getRequestId())
                .metadata(context.getSourceRequest().getMetadata())
                .deltaInformations(context.getDeltaInformations())
                .sqlNode(context.getDmlRequestContext().getSqlNode())
                .envName(context.getDmlRequestContext().getEnvName())
                .parameters(context.getSourceRequest().getQueryRequest().getParameters())
                .build();
    }

    @Override
    public DmlType getType() {
        return DmlType.LLR;
    }

}
