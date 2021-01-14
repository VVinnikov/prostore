package io.arenadata.dtm.query.execution.plugin.adg.service.impl.dml;

import io.arenadata.dtm.cache.service.CacheService;
import io.arenadata.dtm.common.cache.QueryTemplateKey;
import io.arenadata.dtm.common.cache.QueryTemplateValue;
import io.arenadata.dtm.query.calcite.core.service.QueryTemplateExtractor;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.plugin.adg.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adg.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adg.service.QueryExecutorService;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.QueryResultCacheableLlrService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.arenadata.dtm.query.execution.plugin.adg.constants.ColumnFields.*;

@Slf4j
@Service("adgLlrService")
public class AdgLlrService extends QueryResultCacheableLlrService {
    private static final List<String> SYSTEM_FIELDS = Arrays.asList(SYS_FROM_FIELD, SYS_OP_FIELD, SYS_TO_FIELD);
    private final QueryEnrichmentService queryEnrichmentService;
    private final QueryExecutorService executorService;

    @Autowired
    public AdgLlrService(QueryEnrichmentService queryEnrichmentService,
                         QueryExecutorService executorService,
                         @Qualifier("adgQueryTemplateCacheService")
                                 CacheService<QueryTemplateKey, QueryTemplateValue> queryCacheService,
                         @Qualifier("coreQueryTmplateExtractor") QueryTemplateExtractor templateExtractor,
                         @Qualifier("adgSqlDialect") SqlDialect sqlDialect) {
        super(queryCacheService, templateExtractor, sqlDialect);
        this.queryEnrichmentService = queryEnrichmentService;
        this.executorService = executorService;
    }

    @Override
    protected Future<List<Map<String, Object>>> queryExecute(String enrichedQuery, List<ColumnMetadata> metadata) {
        return executorService.execute(enrichedQuery, metadata);
    }

    @Override
    protected Future<String> enrichQuery(LlrRequest llrRequest) {
        val enrichQueryRequest =
                EnrichQueryRequest.generate(llrRequest.getQueryRequest(), llrRequest.getSchema(), llrRequest.getSqlNode());
        return queryEnrichmentService.enrich(enrichQueryRequest);
    }

    @Override
    protected List<String> ignoredSystemFieldsInTemplate() {
        return SYSTEM_FIELDS;
    }
}
