package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.dml;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.QueryTemplateValue;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.reader.QueryParameters;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adqm.service.TemplateParameterConverter;
import io.arenadata.dtm.query.execution.plugin.adqm.utils.Constants;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.QueryResultCacheableLlrService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
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

    @Autowired
    public AdqmLlrService(QueryEnrichmentService queryEnrichmentService,
                          @Qualifier("adqmQueryExecutor") DatabaseExecutor adqmQueryExecutor,
                          @Qualifier("adqmQueryTemplateCacheService")
                                  CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService,
                          @Qualifier("adqmQueryTemplateExtractor") QueryTemplateExtractor templateExtractor,
                          @Qualifier("adqmSqlDialect") SqlDialect sqlDialect,
                          @Qualifier("adqmTemplateParameterConverter") TemplateParameterConverter templateParameterConverter) {
        super(queryCacheService, templateExtractor, sqlDialect);
        this.queryEnrichmentService = queryEnrichmentService;
        this.executorService = adqmQueryExecutor;
        this.templateParameterConverter = templateParameterConverter;
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

    // TODO add query caching support for ADQM
    @Override
    public Future<QueryResult> execute(LlrRequest request) {
        return Future.future(promise -> enrichQuery(request)
                .compose(enrichedQuery -> queryExecute(enrichedQuery, request.getParameters(), request.getMetadata()))
                .map(result -> QueryResult.builder()
                        .requestId(request.getRequestId())
                        .metadata(request.getMetadata())
                        .result(result)
                        .build())
                .onComplete(promise));
    }

    @Override
    protected Future<String> enrichQuery(LlrRequest llrRequest) {
        return queryEnrichmentService.enrich(EnrichQueryRequest.builder()
                .query(llrRequest.getSqlNode())
                .deltaInformations(llrRequest.getDeltaInformations())
                .envName(llrRequest.getEnvName())
                .schema(llrRequest.getSchema())
                .build());
    }

    @Override
    protected List<SqlNode> convertParams(List<SqlNode> params) {
        return templateParameterConverter.convert(params);
    }

    @Override
    protected List<String> ignoredSystemFieldsInTemplate() {
        return SYSTEM_FIELDS;
    }
}
