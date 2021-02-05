package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.PreparedQueryKey;
import io.arenadata.dtm.common.cache.PreparedQueryValue;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.SourceQueryTemplateValue;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.*;
import io.arenadata.dtm.query.calcite.core.extension.dml.DmlType;
import io.arenadata.dtm.query.calcite.core.service.DeltaQueryPreprocessor;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import io.arenadata.dtm.query.calcite.core.util.SqlNodeUtil;
import io.arenadata.dtm.query.execution.core.dto.dml.DmlRequestContext;
import io.arenadata.dtm.query.execution.core.dto.dml.LlrRequestContext;
import io.arenadata.dtm.query.execution.core.exception.query.QueriedEntityIsMissingException;
import io.arenadata.dtm.query.execution.core.factory.LlrRequestContextFactory;
import io.arenadata.dtm.query.execution.core.service.datasource.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.dml.*;
import io.arenadata.dtm.query.execution.core.service.metrics.MetricsService;
import io.arenadata.dtm.query.execution.core.service.schema.LogicalSchemaProvider;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
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
    private final LlrRequestContextFactory llrRequestContextFactory;

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
                          @Qualifier("corePreparedQueryCacheService") CacheService<PreparedQueryKey, PreparedQueryValue> preparedQueryCacheService,
                          LlrRequestContextFactory llrRequestContextFactory) {
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
        this.llrRequestContextFactory = llrRequestContextFactory;
    }

    public Future<QueryResult> execute2(DmlRequestContext context) {
        val queryRequest = context.getRequest().getQueryRequest();
        val sourceRequest = new QuerySourceRequest(queryRequest, context.getSqlNode(), context.getSourceType());
        LlrRequestContext llrContext = LlrRequestContext.builder()
                .originalQuery(context.getSqlNode())
                .sourceRequest(sourceRequest)
                .dmlRequestContext(context)
                .build();
        if (queryRequest.isPrepare()) {
            String sql = llrContext.getSourceRequest().getQueryRequest().getSql();
            SqlNode sqlNode = llrContext.getDmlRequestContext().getSqlNode();
            cachePreparedStatementQuery(sql, sqlNode);
            return replaceViewFromSqlNode(llrContext)
                    .compose(v -> prepareQuery(llrContext));
        } else {
            return replaceViewFromSqlNode(llrContext)
                    .compose(v -> initDeltaInformations(llrContext))
                    .compose(v -> initLlrContext(llrContext))
                    .compose(v -> defineQueryAndExecute(llrContext));
        }
    }

    public Future<QueryResult> execute(DmlRequestContext context) {
        return Future.future((Promise<QueryResult> promise) -> {
            val queryRequest = context.getRequest().getQueryRequest();
            val sql = queryRequest.getSql();
            val sqlNode = context.getSqlNode();
            val datamart = queryRequest.getDatamartMnemonic();
            if (queryRequest.isPrepare()) {
                cachePreparedStatementQuery(sql, sqlNode);
                logicViewReplacer.replace(sqlNode, datamart)
                        .map(sqlNodeWithoutViews -> {
                            queryRequest.setSql(sqlNodeWithoutViews.toSqlString(SqlDialect.CALCITE).toString());//FIXME use bean SqlDialect
                            val originalNode = context.getSqlNode();
                            context.setSqlNode(sqlNodeWithoutViews);
                            return originalNode;
                        })
                        .compose(originalNode -> createLlrRequestContext(originalNode, context))
                        .compose(this::initQuerySourceTypeAndUpdateQueryCacheIfNeeded)
                        .compose(llrRequestContext -> dataSourcePluginService.prepareLlr(defineSourceType(llrRequestContext),
                                llrRequestContext.getDmlRequestContext().getMetrics(),
                                createLlrRequest(llrRequestContext)))
                        .map(v -> QueryResult.emptyResult())
                        .onComplete(promise);
            } else {
                logicViewReplacer.replace(sqlNode, datamart)
                        .map(sqlNodeWithoutViews -> {
                            queryRequest.setSql(sqlNodeWithoutViews.toSqlString(SqlDialect.CALCITE).toString());//FIXME use bean SqlDialect
                            return sqlNodeWithoutViews;
                        })
                        .compose(sqlNodeWithoutViews -> execute(sqlNodeWithoutViews, context))
                        .onComplete(promise);
            }
        });
    }

    private Future<QueryResult> execute(SqlNode sqlNodeWithoutViews, DmlRequestContext context) {
        return Future.future(promise -> {
            val originalNode = context.getSqlNode();
            context.setSqlNode(sqlNodeWithoutViews);
            deltaQueryPreprocessor.process(context.getSqlNode())
                    .compose(deltaResponse -> {
                        if (informationSchemaDefinitionService.isInformationSchemaRequest(deltaResponse.getDeltaInformations())) {
                            //FIXME redundant initialization of deltainformations in factory class
                            return Future.future((Promise<QueryResult> p) -> llrRequestContextFactory.create(context)
                                    .map(llrRequestContext -> {
                                        llrRequestContext.setOriginalQuery(originalNode);
                                        llrRequestContext.getSourceRequest().setQuery(context.getSqlNode());
                                        return llrRequestContext;
                                    }).compose(llrRequestContext ->
                                            metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                                                    SqlProcessingType.LLR,
                                                    llrRequestContext.getDmlRequestContext().getMetrics())
                                                    .compose(v -> informationSchemaDefinitionService.checkAccessToSystemLogicalTables(llrRequestContext.getOriginalQuery()))
                                                    .compose(v -> informationSchemaExecutor.execute(llrRequestContext.getSourceRequest()))
                                                    .onComplete(metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                                                            SqlProcessingType.LLR,
                                                            llrRequestContext.getDmlRequestContext().getMetrics(),
                                                            p))));
                        } else {
                            return createLlrRequestContext(originalNode, context)
                                    .compose(this::initQuerySourceTypeAndUpdateQueryCacheIfNeeded)
                                    .compose(llrRequestContext -> dataSourcePluginService.llr(defineSourceType(llrRequestContext),
                                            llrRequestContext.getDmlRequestContext().getMetrics(),
                                            createLlrRequest(llrRequestContext)));
                        }
                    })
                    .onComplete(promise);
        });
    }

    private void cachePreparedStatementQuery(String sql, SqlNode sqlNode) {
        preparedQueryCacheService.put(new PreparedQueryKey(sql), new PreparedQueryValue(sqlNode));
    }

    private Future<QueryResult> prepareQuery(LlrRequestContext llrContext) {
        return Future.future(promise -> {
            getQueryTemplateValueFromCacheOrCreate(llrContext)
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

    private Future<LlrRequestContext> createLlrRequestContext(SqlNode originalNode, DmlRequestContext context) {
        val templateResult = createQueryTemplateResult(context.getSqlNode());
        Optional<SourceQueryTemplateValue> sourceQueryTemplateValueOpt =
                Optional.ofNullable(queryCacheService.get(QueryTemplateKey.builder()
                        .sourceQueryTemplate(templateResult.getTemplate())
                        .build()));
        if (sourceQueryTemplateValueOpt.isPresent()) {
            val queryTemplateValue = sourceQueryTemplateValueOpt.get();
            return llrRequestContextFactory.create(context, queryTemplateValue)
                    .map(llrRequestContext -> {
                        llrRequestContext.getSourceRequest().setQueryTemplate(templateResult);
                        //llrRequestContext.getSourceRequest().setQuery(context.getSqlNode());
                        llrRequestContext.setOriginalQuery(originalNode);
                        return llrRequestContext;
                    });
        } else {
            return llrRequestContextFactory.create(context)
                    .map(llrRequestContext -> {
                        llrRequestContext.getSourceRequest().setQueryTemplate(templateResult);
                        //llrRequestContext.getSourceRequest().setQuery(context.getSqlNode());
                        llrRequestContext.setOriginalQuery(originalNode);
                        return llrRequestContext;
                    })
                    .compose(this::cacheQueryTemplateValue);
        }
    }

    private Future<LlrRequestContext> cacheQueryTemplateValue(LlrRequestContext llrRequestContext) {
        val newQueryTemplateKey = QueryTemplateKey.builder().build();
        val newQueryTemplateValue = SourceQueryTemplateValue.builder().build();
        llrRequestContext.setQueryTemplateValue(newQueryTemplateValue);
        return Future.future(promise -> {
            newQueryTemplateKey.setSourceQueryTemplate(llrRequestContext.getSourceRequest().getQueryTemplate().getTemplate());
            newQueryTemplateKey.setLogicalSchema(llrRequestContext.getSourceRequest().getLogicalSchema());
            newQueryTemplateValue.setDeltaInformations(llrRequestContext.getDeltaInformations());
            newQueryTemplateValue.setMetadata(llrRequestContext.getSourceRequest().getMetadata());
            newQueryTemplateValue.setLogicalSchema(llrRequestContext.getSourceRequest().getLogicalSchema());
            newQueryTemplateValue.setSql(llrRequestContext.getSourceRequest().getQueryRequest().getSql());
            targetDatabaseDefinitionService.getAcceptableSourceTypes(llrRequestContext.getSourceRequest())
                    .map(sourceTypes -> {
                        newQueryTemplateValue.setAvailableSourceTypes(sourceTypes);
                        return sourceTypes;
                    })
                    .compose(v -> queryCacheService.put(QueryTemplateKey.builder()
                                    .sourceQueryTemplate(llrRequestContext.getSourceRequest().getQueryTemplate().getTemplate())
                                    .logicalSchema(llrRequestContext.getSourceRequest().getLogicalSchema())
                                    .build(),
                            llrRequestContext.getQueryTemplateValue()))
                    .map(v -> llrRequestContext)
                    .onComplete(promise);
        });
    }

    private Future<LlrRequestContext> initQuerySourceTypeAndUpdateQueryCacheIfNeeded(LlrRequestContext llrContext) {
        if (llrContext.getSourceRequest().getSourceType() == null
                && llrContext.getQueryTemplateValue().getLeastQueryCostSourceType() == null) {
            return targetDatabaseDefinitionService.getSourceTypeWithLeastQueryCost(llrContext.getQueryTemplateValue().getAvailableSourceTypes(),
                    llrContext.getSourceRequest())
                    .compose(leastQueryCostSourceType -> {
                                llrContext.getQueryTemplateValue().setLeastQueryCostSourceType(leastQueryCostSourceType);
                                return queryCacheService.put(QueryTemplateKey.builder()
                                                .sourceQueryTemplate(llrContext.getSourceRequest().getQueryTemplate().getTemplate())
                                                .logicalSchema(llrContext.getSourceRequest().getLogicalSchema())
                                                .build(),
                                        llrContext.getQueryTemplateValue());
                            }
                    )
                    .map(v -> llrContext);
        } else if (llrContext.getSourceRequest().getSourceType() != null
                && !llrContext.getQueryTemplateValue().getAvailableSourceTypes().contains(llrContext.getSourceRequest().getSourceType())) {
            return Future.failedFuture(new QueriedEntityIsMissingException(llrContext.getSourceRequest().getSourceType()));
        } else {
            return Future.succeededFuture(llrContext);
        }
    }

    private SourceType defineSourceType(LlrRequestContext llrRequestContext) {
        return llrRequestContext.getSourceRequest().getSourceType() == null ?
                llrRequestContext.getQueryTemplateValue().getLeastQueryCostSourceType() :
                llrRequestContext.getSourceRequest().getSourceType();
    }

    private Optional<SourceQueryTemplateValue> getQueryTemplateValue(LlrRequestContext context) {
        val templateResult = createQueryTemplateResult(context.getDmlRequestContext().getSqlNode());
        val queryTemplateValue = queryCacheService.get(QueryTemplateKey.builder()
                .sourceQueryTemplate(templateResult.getTemplate())
                .build());
        context.getSourceRequest().setQueryTemplate(templateResult);
        return Optional.ofNullable(queryTemplateValue);
    }

    private QueryTemplateResult createQueryTemplateResult(SqlNode sqlNode) {
        val copySqlNode = SqlNodeUtil.copy(sqlNode);
        return templateExtractor.extract(copySqlNode);
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

    private Future<QueryResult> defineQueryAndExecute(LlrRequestContext llrContext) {
        return Future.future(promise -> {
            if (informationSchemaDefinitionService.isInformationSchemaRequest(llrContext.getDeltaInformations())) {
                llrContext.getSourceRequest().setQuery(llrContext.getDmlRequestContext().getSqlNode());//FIXME check correct behaviour
                metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                        SqlProcessingType.LLR,
                        llrContext.getDmlRequestContext().getMetrics())
                        .compose(v -> informationSchemaDefinitionService.checkAccessToSystemLogicalTables(llrContext.getOriginalQuery()))
                        .compose(v -> informationSchemaExecutor.execute(llrContext.getSourceRequest()))
                        .onComplete(metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                                SqlProcessingType.LLR,
                                llrContext.getDmlRequestContext().getMetrics(),
                                promise));
            } else {
                Optional<SourceQueryTemplateValue> queryTemplateValue = getQueryTemplateValue(llrContext);
                execute(queryTemplateValue, llrContext)
                        .onComplete(promise);
            }
        });
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
                executeRequest(llrContext)
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
                    llrContext.setDeltaInformations(preprocessorResponse.getDeltaInformations());
                    llrContext.getDmlRequestContext().setSqlNode(preprocessorResponse.getSqlNode());
                    return llrContext;
                });
    }

    private Future<QueryResult> executeRequest(LlrRequestContext llrContext) {
        return Future.future(promise -> {
            val newQueryTemplateKey = QueryTemplateKey.builder().build();
            val newQueryTemplateValue = SourceQueryTemplateValue.builder().build();
            initLlrContext(llrContext)//TODO check and remove as redundant
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
