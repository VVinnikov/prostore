package io.arenadata.dtm.query.execution.plugin.adb.service.impl.dml;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adb.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.LlrService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("adbLlrService")
@Slf4j
public class AdbLlrService implements LlrService<QueryResult> {

    private final QueryEnrichmentService adbQueryEnrichmentService;
    private final DatabaseExecutor adbDatabaseExecutor;

    @Autowired
    public AdbLlrService(QueryEnrichmentService adbQueryEnrichmentService,
                         @Qualifier("adbQueryExecutor") DatabaseExecutor adbDatabaseExecutor) {
        this.adbQueryEnrichmentService = adbQueryEnrichmentService;
        this.adbDatabaseExecutor = adbDatabaseExecutor;
    }

    @Override
    public Future<QueryResult> execute(LlrRequestContext context) {
        LlrRequest request = context.getRequest();
        EnrichQueryRequest enrichQueryRequest = EnrichQueryRequest.generate(request.getQueryRequest(),
                request.getSchema());
        return adbQueryEnrichmentService.enrich(enrichQueryRequest)
                .compose(enrichQuery -> adbDatabaseExecutor.execute(enrichQuery, request.getMetadata()))
                .map(result -> QueryResult.builder()
                        .requestId(request.getQueryRequest().getRequestId())
                        .metadata(request.getMetadata())
                        .result(result)
                        .build());
    }
}
