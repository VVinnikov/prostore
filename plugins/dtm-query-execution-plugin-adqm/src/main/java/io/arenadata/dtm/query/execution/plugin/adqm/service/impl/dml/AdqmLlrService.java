package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.dml;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.QueryTemplateValue;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.QueryTemplateResult;
import io.arenadata.dtm.query.calcite.core.dto.EnrichmentTemplateRequest;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adqm.utils.Constants;
import io.arenadata.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.LlrService;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.ArrayList;

@Service("adqmLlrService")
@Slf4j
public class AdqmLlrService implements LlrService<QueryResult> {

    private final QueryEnrichmentService queryEnrichmentService;
    private final DatabaseExecutor queryExecutor;
    private final CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService;
    private final QueryTemplateExtractor templateExtractor;
    private final SqlDialect sqlDialect;

    @Autowired
    public AdqmLlrService(QueryEnrichmentService queryEnrichmentService,
                         @Qualifier("adqmQueryExecutor") DatabaseExecutor adbDatabaseExecutor,
                         @Qualifier("adqmQueryTemplateCacheService")
                                 CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService,
                         @Qualifier("coreQueryTmplateExtractor") QueryTemplateExtractor templateExtractor,
                         @Qualifier("adqmSqlDialect") SqlDialect sqlDialect) {
        this.queryEnrichmentService = queryEnrichmentService;
        this.queryExecutor = adbDatabaseExecutor;
        this.queryCacheService = queryCacheService;
        this.templateExtractor = templateExtractor;
        this.sqlDialect = sqlDialect;
    }

    @Override
    public Future<QueryResult> execute(LlrRequestContext llrCtx) {
        return Future.future(promise -> getQueryFromCacheOrInit(llrCtx)
                .compose(enrichedQuery -> queryExecutor.execute(enrichedQuery,
                        llrCtx.getRequest().getMetadata()))
                .map(result -> QueryResult.builder()
                        .requestId(llrCtx.getRequest().getQueryRequest().getRequestId())
                        .metadata(llrCtx.getRequest().getMetadata())
                        .result(result)
                        .build())
                .onComplete(promise));
    }

    private Future<String> getQueryFromCacheOrInit(LlrRequestContext llrCtx) {
        return Future.future(promise -> {
            val queryTemplateValue = getQueryTemplateValueFromCache(llrCtx);
            if (queryTemplateValue != null) {
                promise.complete(getEnrichmentSqlFromTemplate(llrCtx, queryTemplateValue));
            } else {
                val llrRequest = llrCtx.getRequest();
                val enrichQueryRequest =
                        EnrichQueryRequest.generate(llrRequest.getQueryRequest(), llrRequest.getSchema());
                queryEnrichmentService.enrich(enrichQueryRequest)
                        .compose(enrichRequest -> Future.future((Promise<String> p) -> {
                            val template = extractTemplateWithoutSystemFields(enrichRequest);
                            queryCacheService.put(getQueryTemplateKey(llrCtx), getQueryTemplateValue(template))
                                    .map(r -> enrichRequest)
                                    .onComplete(p);
                        }))
                        .onComplete(promise);
            }
        });
    }

    private QueryTemplateValue getQueryTemplateValueFromCache(LlrRequestContext llrCtx) {
        val template = llrCtx.getRequest().getSourceQueryTemplateResult().getTemplate();
        val queryTemplateKey = QueryTemplateKey.builder()
                .sourceQueryTemplate(template)
                .build();
        return queryCacheService.get(queryTemplateKey);
    }

    private QueryTemplateResult extractTemplateWithoutSystemFields(String enrichRequest) {
        return templateExtractor.extract(enrichRequest,
                new ArrayList<>(Constants.SYSTEM_FIELDS));
    }

    private QueryTemplateValue getQueryTemplateValue(QueryTemplateResult templateResult) {
        return QueryTemplateValue.builder()
                .enrichQueryTemplateNode(templateResult.getTemplateNode())
                .build();
    }

    private QueryTemplateKey getQueryTemplateKey(LlrRequestContext context) {
        return QueryTemplateKey.builder()
                .sourceQueryTemplate(context.getRequest().getSourceQueryTemplateResult().getTemplate())
                .logicalSchema(context.getRequest().getSchema())
                .build();
    }

    private String getEnrichmentSqlFromTemplate(LlrRequestContext context, QueryTemplateValue queryTemplateValue) {
        val params = context.getRequest().getSourceQueryTemplateResult().getParams();
        val enrichQueryTemplateNode = queryTemplateValue.getEnrichQueryTemplateNode();
        val enrichTemplate =
                templateExtractor.enrichTemplate(new EnrichmentTemplateRequest(enrichQueryTemplateNode, params));
        return enrichTemplate.toSqlString(sqlDialect).getSql();
    }
}
