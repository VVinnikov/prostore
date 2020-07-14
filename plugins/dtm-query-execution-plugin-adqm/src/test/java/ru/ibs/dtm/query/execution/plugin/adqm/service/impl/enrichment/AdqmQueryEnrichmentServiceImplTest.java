package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.enrichment;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.delta.DeltaInformation;
import ru.ibs.dtm.common.dto.ActualDeltaRequest;
import ru.ibs.dtm.common.service.DeltaService;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdqmQueryEnrichmentServiceImplTest {
    private static AdqmQueryEnrichmentServiceImpl enrichmentService;
    private static final Map<String, Long> mockDeltas = new HashMap<>();

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
                new MockDeltaService(), null, null
        );
    }

    @Test
    public void testCalcDeltaValues() {
        List<DeltaInformation> deltas = Arrays.asList(
                new DeltaInformation("t1", "2020-01-01 16:00:00", false, 0L, "test", "table1", null),
                new DeltaInformation("t2", "2020-02-01 16:00:00", false, 0L, "test", "table1", null),
                new DeltaInformation("t3", "2020-03-01 16:00:00", false, 0L, "test", "table2", null)
        );

        enrichmentService.calculateDeltaValues(deltas, ar -> {
            assertTrue(ar.succeeded());
            assertEquals(deltas.size(), ar.result().size());

            ar.result().forEach(d ->
                    assertEquals(mockDeltas.get(d.getTableName() + "_" + d.getDeltaTimestamp()), d.getDeltaNum()));
        });
    }
}