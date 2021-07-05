package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.async.AsyncUtils;
import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.QueryTemplateValue;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.common.reader.QueryParameters;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.QueryTemplateResult;
import io.arenadata.dtm.query.calcite.core.dto.EnrichmentTemplateRequest;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.Map;

@Slf4j
public abstract class QueryResultCacheableLlrService implements LlrService<QueryResult> {
    protected final CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService;
    protected final QueryTemplateExtractor templateExtractor;
    protected final SqlDialect sqlDialect;
    private final QueryParserService queryParserService;

    public QueryResultCacheableLlrService(CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService,
                                          QueryTemplateExtractor templateExtractor,
                                          SqlDialect sqlDialect,
                                          QueryParserService queryParserService) {
        this.queryCacheService = queryCacheService;
        this.templateExtractor = templateExtractor;
        this.sqlDialect = sqlDialect;
        this.queryParserService = queryParserService;
    }

    @Override
    public Future<QueryResult> execute(LlrRequest request) {
        return Future.future(promise -> AsyncUtils.measureMs(getQueryFromCacheOrInit(request),
                duration -> log.debug("Got query from cache and enriched template for query [{}] in [{}]ms",
                        request.getRequestId(), duration))
                .compose(enrichedQuery -> queryExecute(enrichedQuery, request.getParameters(), request.getMetadata()))
                .map(result -> QueryResult.builder()
                        .requestId(request.getRequestId())
                        .metadata(request.getMetadata())
                        .result(result)
                        .build())
                .onComplete(promise));
    }

    @Override
    public Future<Void> prepare(LlrRequest request) {
        return Future.future(promise -> queryParserService.parse(new QueryParserRequest(request.getWithoutViewsQuery(), request.getSchema()))
                .map(parserResponse -> {
                    validateQuery(parserResponse);
                    return parserResponse;
                })
                .compose(parserResponse -> enrichQuery(request, parserResponse))
                .compose(enrichedQuery -> Future.future((Promise<String> p) -> {
                    val template = extractTemplateWithoutSystemFields(enrichedQuery);
                    queryCacheService.put(getQueryTemplateKey(request), getQueryTemplateValue(template))
                            .map(r -> enrichedQuery)
                            .onComplete(p);
                }))
                .onSuccess(success -> promise.complete())
                .onFailure(promise::fail));
    }

    protected abstract Future<List<Map<String, Object>>> queryExecute(String enrichedQuery,
                                                                      QueryParameters queryParameters,
                                                                      List<ColumnMetadata> metadata);

    private Future<String> getQueryFromCacheOrInit(LlrRequest llrRq) {
        return Future.future(promise -> {
            val queryTemplateValue = getQueryTemplateValueFromCache(llrRq);
            if (queryTemplateValue != null) {
                promise.complete(getEnrichmentSqlFromTemplate(llrRq, queryTemplateValue));
            } else {
                queryParserService.parse(new QueryParserRequest(llrRq.getWithoutViewsQuery(), llrRq.getSchema()))
                        .map(parserResponse -> {
                            validateQuery(parserResponse);
                            return parserResponse;
                        })
                        .compose(parserResponse -> enrichQuery(llrRq, parserResponse))
                        .compose(enrichRequest -> Future.future((Promise<String> p) -> {
                            val template = extractTemplateWithoutSystemFields(enrichRequest);
                            queryCacheService.put(getQueryTemplateKey(llrRq), getQueryTemplateValue(template))
                                    .map(r -> getEnrichmentSqlFromTemplate(llrRq, getQueryTemplateValue(template)))
                                    .onComplete(p);
                        }))
                        .onComplete(promise);
            }
        });
    }

    protected abstract Future<String> enrichQuery(LlrRequest llrRequest, QueryParserResponse parserResponse);

    protected void validateQuery(QueryParserResponse parserResponse) {
    }

    private QueryTemplateValue getQueryTemplateValueFromCache(LlrRequest llrRq) {
        return queryCacheService.get(getQueryTemplateKey(llrRq));
    }

    private QueryTemplateResult extractTemplateWithoutSystemFields(String enrichRequest) {
        return templateExtractor.extract(enrichRequest, ignoredSystemFieldsInTemplate());
    }

    private QueryTemplateValue getQueryTemplateValue(QueryTemplateResult templateResult) {
        return QueryTemplateValue.builder()
                .enrichQueryTemplateNode(templateResult.getTemplateNode())
                .build();
    }

    private QueryTemplateKey getQueryTemplateKey(LlrRequest llrRq) {
        String template = templateExtractor.extract(llrRq.getOriginalQuery()).getTemplate();
        return QueryTemplateKey.builder()
                .sourceQueryTemplate(template)
                .logicalSchema(llrRq.getSchema())
                .build();
    }

    private String getEnrichmentSqlFromTemplate(LlrRequest llrRq, QueryTemplateValue queryTemplateValue) {
        val params = convertParams(llrRq.getSourceQueryTemplateResult().getParams(), llrRq.getParameterTypes());
        val enrichQueryTemplateNode = queryTemplateValue.getEnrichQueryTemplateNode();
        val enrichTemplate =
                templateExtractor.enrichTemplate(new EnrichmentTemplateRequest(enrichQueryTemplateNode, params));
        return enrichTemplate.toSqlString(sqlDialect).getSql();
    }

    protected abstract List<String> ignoredSystemFieldsInTemplate();

    protected List<SqlNode> convertParams(List<SqlNode> params, List<SqlTypeName> parameterTypes) {
        return params;
    }
}
