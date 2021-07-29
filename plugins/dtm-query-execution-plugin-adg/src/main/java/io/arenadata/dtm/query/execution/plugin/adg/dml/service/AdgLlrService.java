package io.arenadata.dtm.query.execution.plugin.adg.dml.service;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.QueryTemplateValue;
import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.common.reader.QueryParameters;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adg.query.service.QueryExecutorService;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.LlrValidationService;
import io.arenadata.dtm.query.execution.plugin.api.service.QueryResultCacheableLlrService;
import io.arenadata.dtm.query.execution.plugin.api.service.TemplateParameterConverter;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.arenadata.dtm.query.execution.plugin.adg.base.utils.ColumnFields.*;

@Slf4j
@Service("adgLlrService")
public class AdgLlrService extends QueryResultCacheableLlrService {
    private static final List<String> SYSTEM_FIELDS = Arrays.asList(SYS_FROM_FIELD, SYS_OP_FIELD, SYS_TO_FIELD);
    private final QueryEnrichmentService queryEnrichmentService;
    private final QueryExecutorService executorService;
    private final TemplateParameterConverter templateParameterConverter;
    private final LlrValidationService adgValidationService;

    @Autowired
    public AdgLlrService(@Qualifier("adgQueryEnrichmentService") QueryEnrichmentService adgQueryEnrichmentService,
                         QueryExecutorService executorService,
                         @Qualifier("adgQueryTemplateCacheService")
                                 CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService,
                         @Qualifier("adgQueryTemplateExtractor") QueryTemplateExtractor templateExtractor,
                         @Qualifier("adgSqlDialect") SqlDialect sqlDialect,
                         @Qualifier("adgCalciteDMLQueryParserService") QueryParserService queryParserService,
                         @Qualifier("adgTemplateParameterConverter") TemplateParameterConverter templateParameterConverter,
                         @Qualifier("adgValidationService") LlrValidationService adgValidationService) {
        super(queryCacheService, templateExtractor, sqlDialect, queryParserService);
        this.queryEnrichmentService = adgQueryEnrichmentService;
        this.executorService = executorService;
        this.templateParameterConverter = templateParameterConverter;
        this.adgValidationService = adgValidationService;
    }

    @Override
    protected Future<List<Map<String, Object>>> queryExecute(String enrichedQuery,
                                                             QueryParameters queryParameters,
                                                             List<ColumnMetadata> metadata) {
        //FIXME add params
        return executorService.execute(enrichedQuery, queryParameters, metadata);
    }

    @Override
    protected void validateQuery(QueryParserResponse parserResponse) {
        adgValidationService.validate(parserResponse);
    }

    @Override
    protected Future<String> enrichQuery(LlrRequest request, QueryParserResponse parserResponse) {
        return queryEnrichmentService.enrich(EnrichQueryRequest.builder()
                .deltaInformations(request.getDeltaInformations())
                .envName(request.getEnvName())
                .query(request.getWithoutViewsQuery())
                .schema(request.getSchema())
                .build(), parserResponse);
    }

    @Override
    protected List<String> ignoredSystemFieldsInTemplate() {
        return SYSTEM_FIELDS;
    }

    @Override
    protected List<SqlNode> convertParams(List<SqlNode> params, List<SqlTypeName> parameterTypes) {
        return templateParameterConverter.convert(params, parameterTypes);
    }

}
