package io.arenadata.dtm.query.execution.core.edml.mppw.service.impl;

import io.arenadata.dtm.query.execution.core.delta.dto.request.BreakMppwRequest;
import io.arenadata.dtm.query.execution.core.edml.mppw.dto.MppwStopReason;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BreakMppwContext {

    private static final Map<BreakMppwRequest, MppwStopReason> breakReasons = new ConcurrentHashMap<>();

    public static void requestRollback(String datamart, long sysCn, MppwStopReason reason) {
        breakReasons.put(new BreakMppwRequest(datamart, sysCn), reason);
    }

    public static boolean rollbackRequested(String datamart, long sysCn) {
        return breakReasons.containsKey(new BreakMppwRequest(datamart, sysCn));
    }

    public static void removeTask(String datamart, long sysCn) {
        BreakMppwRequest request = new BreakMppwRequest(datamart, sysCn);
        breakReasons.remove(request);
    }

    public static MppwStopReason getReason(String datamart, long sysCn) {
        return breakReasons.get(new BreakMppwRequest(datamart, sysCn));
    }

    public static long getNumberOfTasksByDatamart(String datamart) {
        return breakReasons.keySet().stream().filter(task -> task.getDatamart().equals(datamart)).count();
    }

}
