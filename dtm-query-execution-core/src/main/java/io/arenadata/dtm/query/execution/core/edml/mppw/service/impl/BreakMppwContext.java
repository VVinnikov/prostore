package io.arenadata.dtm.query.execution.core.edml.mppw.service.impl;

import io.arenadata.dtm.query.execution.core.delta.dto.request.BreakMppwRequest;
import io.arenadata.dtm.query.execution.core.edml.mppw.dto.MppwStopReason;
import io.vertx.core.impl.ConcurrentHashSet;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class BreakMppwContext {

    private static final Set<BreakMppwRequest> breakRequests = new ConcurrentHashSet<>();
    private static final Map<BreakMppwRequest, MppwStopReason> breakReasons = new ConcurrentHashMap<>();

    public static void requestRollback(String datamart, long sysCn, MppwStopReason reason) {
        BreakMppwRequest request = buildRequestFor(datamart, sysCn);
        breakRequests.add(request);
        breakReasons.put(request, reason);
    }

    public static boolean rollbackRequested(String datamart, long sysCn) {
        return breakRequests.contains(buildRequestFor(datamart, sysCn));
    }

    public static void removeTask(String datamart, long sysCn) {
        BreakMppwRequest request = buildRequestFor(datamart, sysCn);
        breakRequests.remove(request);
        breakReasons.remove(request);
    }

    public static MppwStopReason getReason(String datamart, long sysCn) {
        return breakReasons.get(buildRequestFor(datamart, sysCn));
    }

    public static long getNumberOfTasksByDatamart(String datamart) {
        return breakRequests.stream().filter(task -> task.getDatamart().equals(datamart)).count();
    }

    private static BreakMppwRequest buildRequestFor(String datamart, long sysCn) {
        return BreakMppwRequest
                .builder()
                .datamart(datamart)
                .sysCn(sysCn)
                .build();
    }

}
