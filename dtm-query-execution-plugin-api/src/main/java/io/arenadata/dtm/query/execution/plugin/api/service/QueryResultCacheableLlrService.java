package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.QueryTemplateValue;
import io.arenadata.dtm.common.reader.QueryParameters;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.QueryTemplateResult;
import io.arenadata.dtm.query.calcite.core.dto.EnrichmentTemplateRequest;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Map;

public abstract class QueryResultCacheableLlrService implements LlrService<QueryResult> {
    protected final CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService;
    protected final QueryTemplateExtractor templateExtractor;
    protected final SqlDialect sqlDialect;

    public QueryResultCacheableLlrService(CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService,
                                          QueryTemplateExtractor templateExtractor,
                                          SqlDialect sqlDialect) {
        this.queryCacheService = queryCacheService;
        this.templateExtractor = templateExtractor;
        this.sqlDialect = sqlDialect;
    }

    @Override
    public Future<QueryResult> execute(LlrRequest request) {
        return Future.future(promise -> getQueryFromCacheOrInit(request)
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
        return Future.future(promise -> enrichQuery(request)
                .compose(enrichRequest -> Future.future((Promise<String> p) -> {
                    val template = extractTemplateWithoutSystemFields(enrichRequest);
                    queryCacheService.put(getQueryTemplateKey(request), getQueryTemplateValue(template))
                            .map(r -> enrichRequest)
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
                enrichQuery(llrRq)
                        .compose(enrichRequest -> Future.future((Promise<String> p) -> {
                            val template = extractTemplateWithoutSystemFields(enrichRequest);
                            QueryTemplateValue templateValue = getQueryTemplateValue(template);
                            queryCacheService.put(getQueryTemplateKey(llrRq), templateValue)
                                    .map(r -> getEnrichmentSqlFromTemplate(llrRq, templateValue))
                                    .onComplete(p);
                        }))
                        .onComplete(promise);
            }
        });
    }

    protected abstract Future<String> enrichQuery(LlrRequest llrRequest);

    private QueryTemplateValue getQueryTemplateValueFromCache(LlrRequest llrRq) {
        val template = llrRq.getSourceQueryTemplateResult().getTemplate();
        val queryTemplateKey = QueryTemplateKey.builder()
                .sourceQueryTemplate(template)
                .build();
        return queryCacheService.get(queryTemplateKey);
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
        return QueryTemplateKey.builder()
                .sourceQueryTemplate(llrRq.getSourceQueryTemplateResult().getTemplate())
                .logicalSchema(llrRq.getSchema())
                .build();
    }

    private String getEnrichmentSqlFromTemplate(LlrRequest llrRq, QueryTemplateValue queryTemplateValue) {
        val params = convertParams(llrRq.getSourceQueryTemplateResult().getParams());
        val enrichQueryTemplateNode = queryTemplateValue.getEnrichQueryTemplateNode();
        val enrichTemplate =
                templateExtractor.enrichTemplate(new EnrichmentTemplateRequest(enrichQueryTemplateNode, params));
        return enrichTemplate.toSqlString(sqlDialect).getSql();
    }

    protected abstract List<String> ignoredSystemFieldsInTemplate();

    protected List<SqlNode> convertParams(List<SqlNode> params) {
        return params;
    }
}
