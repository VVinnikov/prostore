package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.enrichment;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.dto.ActualDeltaRequest;
import ru.ibs.dtm.common.service.DeltaService;
import ru.ibs.dtm.query.execution.plugin.adqm.service.impl.query.QueryPreprocessor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class AdqmQueryEnrichmentServiceImplTest {
    private static AdqmQueryEnrichmentServiceImpl enrichmentService;
    private static Map<String, Long> mockDeltas = new HashMap<>();

    private static class MockDeltaService implements DeltaService {

        @Override
        public void getDeltaOnDateTime(ActualDeltaRequest actualDeltaRequest, Handler<AsyncResult<Long>> resultHandler) {
            resultHandler.handle(Future.succeededFuture(0L));
        }

        @Override
        public void getDeltasOnDateTimes(List<ActualDeltaRequest> actualDeltaRequests, Handler<AsyncResult<List<Long>>> resultHandler) {
            List<Long> result = actualDeltaRequests.stream().map(r ->
                    mockDeltas.get(r.getDatamart() + "_" + r.getDateTime())).collect(Collectors.toList());
            resultHandler.handle(Future.succeededFuture(result));
        }
    }

    @BeforeAll
    public static void setup() {
        mockDeltas.put("table1_2020-01-01 16:00:00", 101L);
        mockDeltas.put("table1_2020-02-01 16:00:00", 102L);
        mockDeltas.put("table2_2020-03-01 16:00:00", 103L);

        enrichmentService = new AdqmQueryEnrichmentServiceImpl(
                null, new MockDeltaService(), null, null, null
        );
    }

    @Test
    public void testCalcDeltaValues() {
        List<QueryPreprocessor.DeltaTimeInformation> deltas = Arrays.asList(
                new QueryPreprocessor.DeltaTimeInformation("test", "table1", "t1", "2020-01-01 16:00:00"),
                new QueryPreprocessor.DeltaTimeInformation("test", "table1", "t2", "2020-02-01 16:00:00"),
                new QueryPreprocessor.DeltaTimeInformation("test", "table2", "t3", "2020-03-01 16:00:00")
        );

        enrichmentService.calculateDeltaValues(deltas, ar -> {
            assertTrue(ar.succeeded());
            assertEquals(deltas.size(), ar.result().size());

            ar.result().forEach(d ->
                    assertEquals(mockDeltas.get(d.getTableName() + "_" + d.getDeltaTimestamp()), d.getDeltaNum()));
        });
    }
}