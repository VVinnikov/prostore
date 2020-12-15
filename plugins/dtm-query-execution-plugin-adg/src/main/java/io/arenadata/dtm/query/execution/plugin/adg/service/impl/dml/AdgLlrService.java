package io.arenadata.dtm.query.execution.plugin.adg.service.impl.dml;

import io.arenadata.dtm.async.AsyncHandler;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adg.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adg.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adg.service.QueryExecutorService;
import io.arenadata.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.LlrService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("adgLlrService")
@Slf4j
public class AdgLlrService implements LlrService<QueryResult> {

    private final QueryEnrichmentService queryEnrichmentService;
    private final QueryExecutorService executorService;


    @Autowired
    public AdgLlrService(QueryEnrichmentService queryEnrichmentService,
                         QueryExecutorService executorService) {
        this.queryEnrichmentService = queryEnrichmentService;
        this.executorService = executorService;
    }

    @Override
    public void execute(LlrRequestContext context, AsyncHandler<QueryResult> handler) {
        LlrRequest request = context.getRequest();
        EnrichQueryRequest enrichQueryRequest = EnrichQueryRequest.generate(request.getQueryRequest(),
                request.getSchema());
        queryEnrichmentService.enrich(enrichQueryRequest, enrich -> {
            if (enrich.succeeded()) {
                executorService.execute(enrich.result(), request.getMetadata(), exec -> {
                    if (exec.succeeded()) {
                        handler.handleSuccess(
                                QueryResult.builder()
                                        .requestId(request.getQueryRequest().getRequestId())
                                        .metadata(request.getMetadata())
                                        .result(exec.result())
                                        .build());
                    } else {
                        handler.handleError(exec.cause());
                    }
                });
            } else {
                handler.handleError(enrich.cause());
            }
        });
    }
}
