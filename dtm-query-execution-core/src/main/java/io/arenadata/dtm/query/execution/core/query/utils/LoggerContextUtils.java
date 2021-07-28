package io.arenadata.dtm.query.execution.core.query.utils;

import io.reactiverse.contextual.logging.ContextualData;

import java.util.UUID;

public final class LoggerContextUtils {
    private static final String REQUEST_ID = "requestId";

    private LoggerContextUtils() {
    }

    public static void setRequestId(UUID requestId) {
        ContextualData.put(REQUEST_ID, requestId.toString());
    }

    public static void setRequestId(String requestId) {
        ContextualData.put(REQUEST_ID, requestId);
    }
}
