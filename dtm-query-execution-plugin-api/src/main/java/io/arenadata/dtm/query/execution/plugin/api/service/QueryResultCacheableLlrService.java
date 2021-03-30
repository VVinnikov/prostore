/*
 * Copyright © 2021 ProStore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.QueryTemplateValue;
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
import org.apache.calcite.sql.type.SqlTypeName;

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
                .compose(enrichedQuery -> queryExecute(enrichedQuery, request.getMetadata()))
                .map(result -> QueryResult.builder()
                        .requestId(request.getRequestId())
                        .metadata(request.getMetadata())
                        .result(result)
                        .build())
                .onComplete(promise));
    }

    protected abstract Future<List<Map<String, Object>>> queryExecute(String enrichedQuery,
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
                            queryCacheService.put(getQueryTemplateKey(llrRq), getQueryTemplateValue(template))
                                    .map(r -> getEnrichmentSqlFromTemplate(llrRq, getQueryTemplateValue(template)))
                                    .onComplete(p);
                        }))
                        .onComplete(promise);
            }
        });
    }

    protected abstract Future<String> enrichQuery(LlrRequest llrRequest);

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
