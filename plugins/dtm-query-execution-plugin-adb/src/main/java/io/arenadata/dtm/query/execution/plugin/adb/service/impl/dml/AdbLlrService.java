package io.arenadata.dtm.query.execution.plugin.adb.service.impl.dml;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.QueryTemplateValue;
import io.arenadata.dtm.common.reader.QueryParameters;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adb.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.QueryResultCacheableLlrService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.arenadata.dtm.query.execution.plugin.adb.factory.Constants.*;
@Slf4j
@Service("adbLlrService")
public class AdbLlrService extends QueryResultCacheableLlrService {

    private static final List<String> SYSTEM_FIELDS = Arrays.asList(SYS_FROM_ATTR, SYS_TO_ATTR, SYS_OP_ATTR);
    private final QueryEnrichmentService queryEnrichmentService;
    private final DatabaseExecutor queryExecutor;

    @Autowired
    public AdbLlrService(QueryEnrichmentService queryEnrichmentService,
                         @Qualifier("adbQueryExecutor") DatabaseExecutor adbDatabaseExecutor,
                         @Qualifier("adbQueryTemplateCacheService")
                                 CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService,
                         @Qualifier("adbQueryTemplateExtractor") QueryTemplateExtractor templateExtractor,
                         @Qualifier("adbSqlDialect") SqlDialect sqlDialect) {
        super(queryCacheService, templateExtractor, sqlDialect);
        this.queryEnrichmentService = queryEnrichmentService;
        this.queryExecutor = adbDatabaseExecutor;
    }

    @Override
    protected Future<List<Map<String, Object>>> queryExecute(String enrichedQuery,
                                                             QueryParameters queryParameters,
                                                             List<ColumnMetadata> metadata) {
        return queryExecutor.executeWithParams(enrichedQuery, queryParameters, metadata);
    }

    @Override
    protected Future<String> enrichQuery(LlrRequest llrRequest) {
        return queryEnrichmentService.enrich(EnrichQueryRequest.builder()
                .deltaInformations(llrRequest.getDeltaInformations())
                .envName(llrRequest.getEnvName())
                .query(llrRequest.getWithoutViewsQuery())
                .schema(llrRequest.getSchema())
                .build());
    }

    @Override
    protected List<String> ignoredSystemFieldsInTemplate() {
        return SYSTEM_FIELDS;
    }
}
