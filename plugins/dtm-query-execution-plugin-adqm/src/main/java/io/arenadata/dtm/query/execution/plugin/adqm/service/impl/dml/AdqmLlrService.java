package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.dml;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.LlrService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("adqmLlrService")
@Slf4j
public class AdqmLlrService implements LlrService<QueryResult> {

    private final QueryEnrichmentService adqmQueryEnrichmentService;
    private final DatabaseExecutor adqmDatabaseExecutor;

    @Autowired
    public AdqmLlrService(@Qualifier("adqmQueryEnrichmentService") QueryEnrichmentService adqmQueryEnrichmentService,
                          @Qualifier("adqmQueryExecutor") DatabaseExecutor adqmDatabaseExecutor) {
        this.adqmQueryEnrichmentService = adqmQueryEnrichmentService;
        this.adqmDatabaseExecutor = adqmDatabaseExecutor;
    }

    @Override
    public Future<QueryResult> execute(LlrRequestContext context) {
        LlrRequest request = context.getRequest();
        EnrichQueryRequest enrichQueryRequest = EnrichQueryRequest.generate(request.getQueryRequest(), request.getSchema());
        return adqmQueryEnrichmentService.enrich(enrichQueryRequest)
                .compose(enrichQuery -> adqmDatabaseExecutor.execute(enrichQuery, request.getMetadata()))
                .map(result ->
                        QueryResult.builder()
                                .requestId(request.getQueryRequest().getRequestId())
                                .metadata(request.getMetadata())
                                .result(result)
                                .build()
                );
    }
}
