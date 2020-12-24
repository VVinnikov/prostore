package io.arenadata.dtm.query.execution.plugin.adb.service.impl.dml;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.QueryTemplateValue;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.QueryTemplateResult;
import io.arenadata.dtm.query.calcite.core.dto.EnrichmentTemplateRequest;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import io.arenadata.dtm.query.execution.plugin.adb.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.LlrService;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Arrays;

import static io.arenadata.dtm.query.execution.plugin.adb.factory.impl.AdbTableEntitiesFactory.*;

@Service("adbLlrService")
@Slf4j
public class AdbLlrService implements LlrService<QueryResult> {

    private final QueryEnrichmentService queryEnrichmentService;
    private final DatabaseExecutor queryExecutor;
    private final CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService;
    private final QueryTemplateExtractor templateExtractor;
    private final SqlDialect sqlDialect;

    @Autowired
    public AdbLlrService(QueryEnrichmentService queryEnrichmentService,
                         @Qualifier("adbQueryExecutor") DatabaseExecutor adbDatabaseExecutor,
                         @Qualifier("adbQueryTemplateCacheService")
                                     CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService,
                         @Qualifier("coreQueryTmplateExtractor") QueryTemplateExtractor templateExtractor,
                         @Qualifier("adbSqlDialect") SqlDialect sqlDialect) {
        this.queryEnrichmentService = queryEnrichmentService;
        this.queryExecutor = adbDatabaseExecutor;
        this.queryCacheService = queryCacheService;
        this.templateExtractor = templateExtractor;
        this.sqlDialect = sqlDialect;
    }

    @Override
    public Future<QueryResult> execute(LlrRequestContext context) {
        return Future.future(promise -> getQueryFromCacheOrInit(context)
                .compose(enrichedQuery -> queryExecutor.execute(enrichedQuery,
                        context.getRequest().getMetadata()))
                .map(result -> QueryResult.builder()
                        .requestId(context.getRequest().getQueryRequest().getRequestId())
                        .metadata(context.getRequest().getMetadata())
                        .result(result)
                        .build())
                .onComplete(promise));
    }

    private Future<String> getQueryFromCacheOrInit(LlrRequestContext context) {
        return Future.future(promise -> {
            LlrRequest request = context.getRequest();
            final QueryTemplateValue queryTemplateValue = queryCacheService.get(QueryTemplateKey.builder()
                    .sourceQueryTemplate(context.getRequest().getSourceQueryTemplateResult().getTemplate())
                    .build());
            if (queryTemplateValue != null) {
                final SqlNode enrichTemplate = templateExtractor.enrichTemplate(
                        new EnrichmentTemplateRequest(queryTemplateValue.getEnrichQueryTemplate(),
                                context.getRequest().getSourceQueryTemplateResult().getParams()));
                promise.complete(enrichTemplate.toSqlString(sqlDialect).getSql());
            } else {
                EnrichQueryRequest enrichQueryRequest = EnrichQueryRequest.generate(request.getQueryRequest(),
                        request.getSchema());
                queryEnrichmentService.enrich(enrichQueryRequest)
                        .compose(enrichRequest -> Future.future((Promise<String> p) -> {
                            final QueryTemplateResult templateResult = templateExtractor.extract(enrichRequest,
                                    Arrays.asList(SYS_FROM_ATTR, SYS_TO_ATTR, SYS_OP_ATTR, REQ_ID_ATTR));
                            queryCacheService.put(QueryTemplateKey.builder()
                                            .sourceQueryTemplate(context.getRequest().getSourceQueryTemplateResult().getTemplate())
                                            .logicalSchema(context.getRequest().getSchema())
                                            .build(),
                                    QueryTemplateValue.builder()
                                            .enrichQueryTemplate(templateResult.getTemplate())
                                            .build())
                                    .map(r -> enrichRequest)
                                    .onComplete(p);
                        }))
                        .onComplete(promise);
            }
        });
    }
}
