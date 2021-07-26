package io.arenadata.dtm.query.execution.core.edml.mppw.service.impl;

import io.arenadata.dtm.query.execution.core.delta.dto.request.BreakMppwRequest;
import io.arenadata.dtm.query.execution.core.edml.mppw.dto.MppwStopReason;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BreakMppwContext {

    private static final Map<BreakMppwRequest, MppwStopReason> breakRequests = new ConcurrentHashMap<>();

    public static void requestRollback(String datamart, long sysCn, MppwStopReason reason) {
        breakRequests.put(new BreakMppwRequest(datamart, sysCn), reason);
    }

    public static boolean rollbackRequested(String datamart, long sysCn) {
        return breakRequests.containsKey(new BreakMppwRequest(datamart, sysCn));
    }

    public static void removeTask(String datamart, long sysCn) {
        breakRequests.remove(new BreakMppwRequest(datamart, sysCn));
    }

    public static MppwStopReason getReason(String datamart, long sysCn) {
        return breakRequests.get(new BreakMppwRequest(datamart, sysCn));
    }

    public static long getNumberOfTasksByDatamart(String datamart) {
        return breakRequests.keySet().stream().filter(task -> task.getDatamart().equals(datamart)).count();
    }

}
