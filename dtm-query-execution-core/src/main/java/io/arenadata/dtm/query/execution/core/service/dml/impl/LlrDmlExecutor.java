package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.PreparedQueryKey;
import io.arenadata.dtm.common.cache.PreparedQueryValue;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.SourceQueryTemplateValue;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.SqlProcessingType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.QueryTemplateResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.dto.delta.DeltaQueryPreprocessorResponse;
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
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@Slf4j
public class LlrDmlExecutor implements DmlExecutor<QueryResult> {

    private final DataSourcePluginService dataSourcePluginService;
    private final AcceptableSourceTypesDefinitionService acceptableSourceTypesService;
    private final DeltaQueryPreprocessor deltaQueryPreprocessor;
    private final LogicViewReplacer logicViewReplacer;
    private final InformationSchemaExecutor infoSchemaExecutor;
    private final InformationSchemaDefinitionService infoSchemaDefService;
    private final MetricsService<RequestMetrics> metricsService;
    private final QueryTemplateExtractor templateExtractor;
    private final CacheService<QueryTemplateKey, SourceQueryTemplateValue> queryCacheService;
    private final CacheService<PreparedQueryKey, PreparedQueryValue> preparedQueryCacheService;
    private final LlrRequestContextFactory llrRequestContextFactory;
    private final SelectCategoryQualifier selectCategoryQualifier;
    private final SuitablePluginSelector suitablePluginSelector;
    private final SqlDialect sqlDialect;
    private final SqlParametersTypeExtractor parametersTypeExtractor;

    @Autowired
    public LlrDmlExecutor(DataSourcePluginService dataSourcePluginService,
                          AcceptableSourceTypesDefinitionService acceptableSourceTypesService,
                          DeltaQueryPreprocessor deltaQueryPreprocessor,
                          LogicViewReplacer logicViewReplacer,
                          InformationSchemaExecutor infoSchemaExecutor,
                          InformationSchemaDefinitionService infoSchemaDefService,
                          MetricsService<RequestMetrics> metricsService,
                          @Qualifier("coreQueryTmplateExtractor") QueryTemplateExtractor templateExtractor,
                          @Qualifier("coreQueryTemplateCacheService") CacheService<QueryTemplateKey, SourceQueryTemplateValue> queryCacheService,
                          @Qualifier("corePreparedQueryCacheService") CacheService<PreparedQueryKey, PreparedQueryValue> preparedQueryCacheService,
                          LlrRequestContextFactory llrRequestContextFactory,
                          SelectCategoryQualifier selectCategoryQualifier,
                          SuitablePluginSelector suitablePluginSelector,
                          @Qualifier("coreSqlDialect") SqlDialect sqlDialect,
                          SqlParametersTypeExtractor parametersTypeExtractor) {
        this.dataSourcePluginService = dataSourcePluginService;
        this.acceptableSourceTypesService = acceptableSourceTypesService;
        this.deltaQueryPreprocessor = deltaQueryPreprocessor;
        this.logicViewReplacer = logicViewReplacer;
        this.infoSchemaExecutor = infoSchemaExecutor;
        this.infoSchemaDefService = infoSchemaDefService;
        this.metricsService = metricsService;
        this.templateExtractor = templateExtractor;
        this.queryCacheService = queryCacheService;
        this.preparedQueryCacheService = preparedQueryCacheService;
        this.llrRequestContextFactory = llrRequestContextFactory;
        this.selectCategoryQualifier = selectCategoryQualifier;
        this.suitablePluginSelector = suitablePluginSelector;
        this.sqlDialect = sqlDialect;
        this.parametersTypeExtractor = parametersTypeExtractor;
    }

    public Future<QueryResult> execute(DmlRequestContext context) {
        return Future.future((Promise<QueryResult> promise) -> {
            val queryRequest = context.getRequest().getQueryRequest();
            val sqlNode = context.getSqlNode();
            if (queryRequest.isPrepare()) {
                prepareQuery(context, queryRequest)
                        .onComplete(promise);
            } else {
                replaceViews(queryRequest, sqlNode)
                        .map(sqlNodeWithoutViews -> {
                            queryRequest.setSql(sqlNodeWithoutViews.toSqlString(sqlDialect).toString());
                            return sqlNodeWithoutViews;
                        })
                        .compose(sqlNodeWithoutViews -> defineQueryAndExecute(sqlNodeWithoutViews, context))
                        .onComplete(promise);
            }
        });
    }

    private Future<QueryResult> prepareQuery(DmlRequestContext context, QueryRequest queryRequest) {
        return Future.future(promise -> {
            val sql = queryRequest.getSql();
            val sqlNode = context.getSqlNode();
            log.debug("Prepare sql query [{}]", sql);
            preparedQueryCacheService.put(new PreparedQueryKey(sql), new PreparedQueryValue(sqlNode));
            replaceViews(queryRequest, sqlNode)
                    .map(sqlNodeWithoutViews -> {
                        val originalNode = context.getSqlNode();
                        context.setSqlNode(sqlNodeWithoutViews);
                        return originalNode;
                    })
                    .compose(originalNode -> createLlrRequestContext(Optional.empty(), originalNode, context))
                    .compose(this::initQuerySourceTypeAndUpdateQueryCacheIfNeeded)
                    .compose(llrRequestContext -> dataSourcePluginService.prepareLlr(defineSourceType(llrRequestContext),
                            llrRequestContext.getDmlRequestContext().getMetrics(),
                            createLlrRequest(llrRequestContext)))
                    .map(v -> QueryResult.emptyResult())
                    .onComplete(promise);
        });
    }

    private Future<SqlNode> replaceViews(QueryRequest queryRequest,
                                         SqlNode sqlNode) {
        return logicViewReplacer.replace(sqlNode, queryRequest.getDatamartMnemonic())
                .map(sqlNodeWithoutViews -> {
                    queryRequest.setSql(sqlNodeWithoutViews.toSqlString(sqlDialect).toString());
                    return sqlNodeWithoutViews;
                });
    }

    private Future<QueryResult> defineQueryAndExecute(SqlNode sqlNodeWithoutViews, DmlRequestContext context) {
        return Future.future(promise -> {
            log.debug("Execute sql query [{}]", context.getRequest().getQueryRequest());
            val originalNode = context.getSqlNode();
            context.setSqlNode(sqlNodeWithoutViews);
            deltaQueryPreprocessor.process(context.getSqlNode())
                    .compose(deltaResponse -> {
                        if (infoSchemaDefService.isInformationSchemaRequest(deltaResponse.getDeltaInformations())) {
                            return executeInformationSchemaRequest(context, originalNode, deltaResponse);
                        } else {
                            return executeLlrRequest(context, originalNode, deltaResponse);
                        }
                    })
                    .onComplete(promise);
        });
    }

    private Future<QueryResult> executeInformationSchemaRequest(DmlRequestContext context,
                                                                SqlNode originalNode,
                                                                DeltaQueryPreprocessorResponse deltaResponse) {
        return initLlrRequestContext(context, originalNode, deltaResponse)
                .compose(this::checkAccessAndExecute);
    }

    private Future<QueryResult> checkAccessAndExecute(LlrRequestContext llrRequestContext) {
        return Future.future(p -> metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                SqlProcessingType.LLR,
                llrRequestContext.getDmlRequestContext().getMetrics())
                .compose(v -> infoSchemaDefService.checkAccessToSystemLogicalTables(llrRequestContext.getOriginalQuery()))
                .compose(v -> infoSchemaExecutor.execute(llrRequestContext.getSourceRequest()))
                .onComplete(metricsService.sendMetrics(SourceType.INFORMATION_SCHEMA,
                        SqlProcessingType.LLR,
                        llrRequestContext.getDmlRequestContext().getMetrics(),
                        p))
        );
    }

    private Future<LlrRequestContext> initLlrRequestContext(DmlRequestContext context,
                                                            SqlNode originalNode,
                                                            DeltaQueryPreprocessorResponse deltaResponse) {
        return llrRequestContextFactory.create(deltaResponse, context)
                .map(llrRequestContext -> {
                    llrRequestContext.setOriginalQuery(originalNode);
                    llrRequestContext.getSourceRequest().setQuery(context.getSqlNode());
                    return llrRequestContext;
                });
    }

    private Future<QueryResult> executeLlrRequest(DmlRequestContext context,
                                                  SqlNode originalNode,
                                                  DeltaQueryPreprocessorResponse deltaResponse) {
        return createLlrRequestContext(Optional.of(deltaResponse), originalNode, context)
                .compose(this::initQuerySourceTypeAndUpdateQueryCacheIfNeeded)
                .compose(llrRequestContext -> dataSourcePluginService.llr(defineSourceType(llrRequestContext),
                        llrRequestContext.getDmlRequestContext().getMetrics(),
                        createLlrRequest(llrRequestContext)));
    }

    private Future<LlrRequestContext> createLlrRequestContext(Optional<DeltaQueryPreprocessorResponse> deltaResponseOpt,
                                                              SqlNode originalNode,
                                                              DmlRequestContext context) {
        val templateResult = createQueryTemplateResult(originalNode);
        Optional<SourceQueryTemplateValue> sourceQueryTemplateValueOpt =
                Optional.ofNullable(queryCacheService.get(QueryTemplateKey.builder()
                        .sourceQueryTemplate(templateResult.getTemplate())
                        .build()));
        if (sourceQueryTemplateValueOpt.isPresent()) {
            val queryTemplateValue = sourceQueryTemplateValueOpt.get();
            log.debug("Found query template cache value by key [{}]", templateResult.getTemplate());
            return llrRequestContextFactory.create(context, queryTemplateValue)
                    .map(llrRequestContext -> {
                        llrRequestContext.getSourceRequest().setQueryTemplate(templateResult);
                        llrRequestContext.setOriginalQuery(originalNode);
                        return llrRequestContext;
                    });
        } else {
            if (deltaResponseOpt.isPresent()) {
                val deltaQueryPreprocessorResponse = deltaResponseOpt.get();
                SqlNode templateNode = templateExtractor.extract(deltaQueryPreprocessorResponse.getSqlNode()).getTemplateNode();
                context.setSqlNode(templateNode);
                return llrRequestContextFactory.create(deltaQueryPreprocessorResponse, context)
                        .map(llrRequestContext -> {
                            llrRequestContext.getSourceRequest().setQueryTemplate(templateResult);
                            llrRequestContext.setOriginalQuery(originalNode);
                            return llrRequestContext;
                        })
                        .compose(this::cacheQueryTemplateValue);
            } else {
                SqlNode templateNode = templateExtractor.extract(context.getSqlNode()).getTemplateNode();
                context.setSqlNode(templateNode);
                return llrRequestContextFactory.create(context)
                        .map(llrRequestContext -> {
                            llrRequestContext.getSourceRequest().setQueryTemplate(templateResult);
                            llrRequestContext.setOriginalQuery(originalNode);
                            return llrRequestContext;
                        })
                        .compose(this::cacheQueryTemplateValue);
            }
        }
    }

    private Future<LlrRequestContext> initQuerySourceTypeAndUpdateQueryCacheIfNeeded(LlrRequestContext llrContext) {
        if (llrContext.getSourceRequest().getSourceType() == null
                && llrContext.getQueryTemplateValue().getMostSuitablePlugin() == null) {
            return Future.future(promise -> {
                val selectCategory = selectCategoryQualifier.qualify(llrContext.getQueryTemplateValue().getLogicalSchema(),
                        llrContext.getDmlRequestContext().getSqlNode());
                val sourceType = suitablePluginSelector.selectByCategory(selectCategory,
                        llrContext.getQueryTemplateValue().getAvailableSourceTypes());
                log.debug("Defined category [{}] for sql query [{}]", selectCategory,
                        llrContext.getDmlRequestContext().getRequest().getQueryRequest().getSql());
                llrContext.getQueryTemplateValue().setSelectCategory(selectCategory);
                llrContext.getQueryTemplateValue().setMostSuitablePlugin(sourceType.orElse(null));
                queryCacheService.put(QueryTemplateKey.builder()
                                .sourceQueryTemplate(llrContext.getSourceRequest().getQueryTemplate().getTemplate())
                                .logicalSchema(llrContext.getSourceRequest().getLogicalSchema())
                                .build(),
                        llrContext.getQueryTemplateValue())
                        .map(v -> llrContext)
                        .onComplete(promise);
            });
        } else if (llrContext.getSourceRequest().getSourceType() != null
                && !llrContext.getQueryTemplateValue().getAvailableSourceTypes()
                .contains(llrContext.getSourceRequest().getSourceType())) {
            return Future.failedFuture(new QueriedEntityIsMissingException(llrContext.getSourceRequest().getSourceType()));
        } else {
            return Future.succeededFuture(llrContext);
        }
    }

    private QueryTemplateResult createQueryTemplateResult(SqlNode sqlNode) {
        val copySqlNode = SqlNodeUtil.copy(sqlNode);
        return templateExtractor.extract(copySqlNode);
    }

    private Future<LlrRequestContext> cacheQueryTemplateValue(LlrRequestContext llrRequestContext) {
        val newQueryTemplateKey = QueryTemplateKey.builder().build();
        val newQueryTemplateValue = SourceQueryTemplateValue.builder().build();
        llrRequestContext.setQueryTemplateValue(newQueryTemplateValue);
        return Future.future(promise -> {
            initQueryTemplate(llrRequestContext, newQueryTemplateKey, newQueryTemplateValue);
            acceptableSourceTypesService.define(llrRequestContext.getSourceRequest())
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

    private void initQueryTemplate(LlrRequestContext llrRequestContext,
                                   QueryTemplateKey newQueryTemplateKey,
                                   SourceQueryTemplateValue newQueryTemplateValue) {
        newQueryTemplateKey.setSourceQueryTemplate(llrRequestContext.getSourceRequest().getQueryTemplate().getTemplate());
        newQueryTemplateKey.setLogicalSchema(llrRequestContext.getSourceRequest().getLogicalSchema());
        newQueryTemplateValue.setDeltaInformations(llrRequestContext.getDeltaInformations());
        newQueryTemplateValue.setMetadata(llrRequestContext.getSourceRequest().getMetadata());
        newQueryTemplateValue.setLogicalSchema(llrRequestContext.getSourceRequest().getLogicalSchema());
        newQueryTemplateValue.setSql(llrRequestContext.getSourceRequest().getQueryRequest().getSql());
        newQueryTemplateValue.setParameterTypes(parametersTypeExtractor.extract(llrRequestContext.getRelNode().rel));
    }

    private SourceType defineSourceType(LlrRequestContext llrRequestContext) {
        SourceType sourceType = llrRequestContext.getSourceRequest().getSourceType() == null ?
                llrRequestContext.getQueryTemplateValue().getMostSuitablePlugin() :
                llrRequestContext.getSourceRequest().getSourceType();
        log.debug("Defined source type [{}] for query [{}]",
                sourceType,
                llrRequestContext.getDmlRequestContext().getRequest().getQueryRequest().getSql());
        return sourceType;
    }

    private LlrRequest createLlrRequest(LlrRequestContext context) {
        QueryRequest queryRequest = context.getDmlRequestContext().getRequest().getQueryRequest();
        return LlrRequest.builder()
                .sourceQueryTemplateResult(context.getSourceRequest().getQueryTemplate())
                .parameters(context.getSourceRequest().getQueryRequest().getParameters())
                .parameterTypes(context.getQueryTemplateValue().getParameterTypes())
                .schema(context.getSourceRequest().getLogicalSchema())
                .sqlNode(context.getDmlRequestContext().getSqlNode())
                .envName(context.getDmlRequestContext().getEnvName())
                .datamartMnemonic(queryRequest.getDatamartMnemonic())
                .metadata(context.getSourceRequest().getMetadata())
                .deltaInformations(context.getDeltaInformations())
                .requestId(queryRequest.getRequestId())
                .build();
    }

    @Override
    public DmlType getType() {
        return DmlType.LLR;
    }

}
