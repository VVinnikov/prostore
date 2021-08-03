package io.arenadata.dtm.query.execution.plugin.adp.dml.service;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.QueryTemplateValue;
import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.common.reader.QueryParameters;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adp.db.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.QueryResultCacheableLlrService;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.arenadata.dtm.query.execution.plugin.adp.base.Constants.*;


@Service("adpLlrService")
public class AdpLlrService extends QueryResultCacheableLlrService {

    private static final List<String> SYSTEM_FIELDS = Arrays.asList(SYS_FROM_ATTR, SYS_TO_ATTR, SYS_OP_ATTR);
    private final QueryEnrichmentService queryEnrichmentService;
    private final DatabaseExecutor queryExecutor;

    @Autowired
    public AdpLlrService(@Qualifier("adpQueryEnrichmentService") QueryEnrichmentService adpQueryEnrichmentService,
                         @Qualifier("adpQueryExecutor") DatabaseExecutor adpDatabaseExecutor,
                         @Qualifier("adpQueryTemplateCacheService")
                                 CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService,
                         @Qualifier("adpQueryTemplateExtractor") QueryTemplateExtractor templateExtractor,
                         @Qualifier("adpSqlDialect") SqlDialect sqlDialect,
                         @Qualifier("adpCalciteDMLQueryParserService") QueryParserService queryParserService) {
        super(queryCacheService, templateExtractor, sqlDialect, queryParserService);
        this.queryEnrichmentService = adpQueryEnrichmentService;
        this.queryExecutor = adpDatabaseExecutor;
    }

    @Override
    protected Future<List<Map<String, Object>>> queryExecute(String enrichedQuery,
                                                             QueryParameters queryParameters,
                                                             List<ColumnMetadata> metadata) {
        return queryExecutor.executeWithParams(enrichedQuery, queryParameters, metadata);
    }

    @Override
    protected void validateQuery(QueryParserResponse parserResponse) {
    }

    @Override
    protected Future<String> enrichQuery(LlrRequest request, QueryParserResponse parserResponse) {
        return queryEnrichmentService.enrich(EnrichQueryRequest.builder()
                        .deltaInformations(request.getDeltaInformations())
                        .envName(request.getEnvName())
                        .query(request.getWithoutViewsQuery())
                        .schema(request.getSchema())
                        .build(),
                parserResponse);
    }

    @Override
    protected List<String> ignoredSystemFieldsInTemplate() {
        return SYSTEM_FIELDS;
    }
}
