package io.arenadata.dtm.query.execution.plugin.adqm.dml.service;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.QueryTemplateValue;
import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.reader.QueryParameters;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adqm.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.base.utils.Constants;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service("adqmLlrService")
@Slf4j
public class AdqmLlrService extends QueryResultCacheableLlrService {

    private static final List<String> SYSTEM_FIELDS = new ArrayList<>(Constants.SYSTEM_FIELDS);
    private final QueryEnrichmentService queryEnrichmentService;
    private final DatabaseExecutor executorService;
    private final TemplateParameterConverter templateParameterConverter;
    private final LlrValidationService adqmValidationService;

    @Autowired
    public AdqmLlrService(@Qualifier("adqmQueryEnrichmentService") QueryEnrichmentService queryEnrichmentService,
                          @Qualifier("adqmQueryExecutor") DatabaseExecutor adqmQueryExecutor,
                          @Qualifier("adqmQueryTemplateCacheService")
                                  CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService,
                          @Qualifier("adqmQueryTemplateExtractor") QueryTemplateExtractor templateExtractor,
                          @Qualifier("adqmSqlDialect") SqlDialect sqlDialect,
                          @Qualifier("adqmCalciteDMLQueryParserService") QueryParserService queryParserService,
                          @Qualifier("adqmTemplateParameterConverter") TemplateParameterConverter templateParameterConverter,
                          @Qualifier("adqmValidationService") LlrValidationService adqmValidationService) {
        super(queryCacheService, templateExtractor, sqlDialect, queryParserService);
        this.queryEnrichmentService = queryEnrichmentService;
        this.executorService = adqmQueryExecutor;
        this.templateParameterConverter = templateParameterConverter;
        this.adqmValidationService = adqmValidationService;
    }

    @Override
    protected Future<List<Map<String, Object>>> queryExecute(String enrichedQuery,
                                                             QueryParameters queryParameters,
                                                             List<ColumnMetadata> metadata) {
        return executorService.executeWithParams(enrichedQuery, getExtendedQueryParameters(queryParameters), metadata);
    }

    private QueryParameters getExtendedQueryParameters(QueryParameters queryParameters) {
        //For adqm enrichment query we have to create x2 params values and their types
        if (queryParameters != null) {
            List<Object> values = new ArrayList<>(queryParameters.getValues());
            List<ColumnType> types = new ArrayList<>(queryParameters.getTypes());
            values.addAll(queryParameters.getValues());
            types.addAll(queryParameters.getTypes());
            return new QueryParameters(values, types);
        } else {
            return null;
        }
    }

    @Override
    protected void validateQuery(QueryParserResponse parserResponse) {
        adqmValidationService.validate(parserResponse);
    }

    @Override
    protected Future<String> enrichQuery(LlrRequest request, QueryParserResponse parserResponse) {
        return queryEnrichmentService.enrich(EnrichQueryRequest.builder()
                        .query(request.getWithoutViewsQuery())
                        .deltaInformations(request.getDeltaInformations())
                        .envName(request.getEnvName())
                        .schema(request.getSchema())
                        .build(),
                parserResponse);
    }

    @Override
    protected List<SqlNode> convertParams(List<SqlNode> params, List<SqlTypeName> parameterTypes) {
        return templateParameterConverter.convert(params, parameterTypes);
    }

    @Override
    protected List<String> ignoredSystemFieldsInTemplate() {
        return SYSTEM_FIELDS;
    }
}
