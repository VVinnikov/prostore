package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.enrichment;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.dto.ActualDeltaRequest;
import ru.ibs.dtm.common.service.DeltaService;
import ru.ibs.dtm.query.execution.plugin.adqm.calcite.CalciteContextProvider;
import ru.ibs.dtm.query.execution.plugin.adqm.dto.DeltaInformation;
import ru.ibs.dtm.query.execution.plugin.adqm.dto.EnrichQueryRequest;
import ru.ibs.dtm.query.execution.plugin.adqm.service.QueryEnrichmentService;
import ru.ibs.dtm.query.execution.plugin.adqm.service.QueryGenerator;
import ru.ibs.dtm.query.execution.plugin.adqm.service.SchemaExtender;
import ru.ibs.dtm.query.execution.plugin.adqm.service.impl.query.QueryPreprocessor;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Service
@Slf4j
public class AdqmQueryEnrichmentServiceImpl implements QueryEnrichmentService {
    private QueryGenerator adqmQueryGenerator;
    private final DeltaService deltaService;
    private final CalciteContextProvider contextProvider;
    private final QueryPreprocessor preprocessor;
    private final SchemaExtender schemaExtender;

    public AdqmQueryEnrichmentServiceImpl(AdqmQueryGeneratorImpl adqmQueryGeneratorimpl,
                                          DeltaService deltaService,
                                          CalciteContextProvider contextProvider,
                                          QueryPreprocessor preprocessor,
                                          @Qualifier("adqmSchemaExtender") SchemaExtender schemaExtender) {
        this.adqmQueryGenerator = adqmQueryGeneratorimpl;
        this.deltaService = deltaService;
        this.contextProvider = contextProvider;
        this.preprocessor = preprocessor;
        this.schemaExtender = schemaExtender;
    }

    @Override
    public void enrich(EnrichQueryRequest request, Handler<AsyncResult<String>> asyncHandler) {
        // FIXME rewrite to the Future's compose instead of callback hell

        // 1. Extract information about deltas via QueryPreprocessor
        preprocessor.process(request.getQueryRequest().getSql(), ar -> {
            if (ar.failed()) {
                asyncHandler.handle(Future.failedFuture(ar.cause()));
            }

            calculateDeltaValues(ar.result(), ar2 -> {
                if (ar2.failed()) {
                    asyncHandler.handle(Future.failedFuture(ar2.cause()));
                }


            });
        });
        // 2. Modify query - add filter for sys_from/sys_to columns based on deltas
        // 3. Modify query - duplicate via union all (with sub queries) and rename table names to physical names
        // 4. Modify query - rename schemas to physical name
    }

    void calculateDeltaValues(List<DeltaInformation> deltas,
                              Handler<AsyncResult<List<DeltaInformation>>> handler) {

        List<ActualDeltaRequest> requests = deltas.stream()
                .map(d -> new ActualDeltaRequest(d.getTableName(), d.getDeltaTimestamp()))
                .collect(Collectors.toList());

        deltaService.getDeltasOnDateTimes(requests, ar -> {
            if (ar.failed()) {
                handler.handle(Future.failedFuture(ar.cause()));
            }

            // we expect what order for returned deltas is the same as in requests list
            // we need to concatenate information about used tables and deltas
            List<Long> deltaNums = ar.result();
            List<DeltaInformation> result = IntStream.range(0, requests.size())
                    .mapToObj(i -> deltas.get(i).withDeltaNum(deltaNums.get(i)))
                    .collect(Collectors.toList());
            handler.handle(Future.succeededFuture(result));
        });
    }
}
