package io.arenadata.dtm.query.execution.core.factory.impl.edml.mppw;

import io.arenadata.dtm.query.execution.core.dto.edml.MppwStopFuture;
import io.arenadata.dtm.query.execution.core.dto.edml.MppwStopReason;
import io.arenadata.dtm.query.execution.core.factory.MppwErrorMessageFactory;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.stereotype.Component;

@Component
public class MppwErrorMessageFactoryImpl implements MppwErrorMessageFactory {
    private static final String OFFSET_RECEIVED_TEMPLATE = "Plugin: %s, status: %s, offset: %d";
    private static final String WITH_ERROR_REASON_TEMPLATE = "Plugin: %s, status: %s, offset: %d, reason: %s";

    @Override
    public String create(MppwStopFuture stopFuture) {
        if (MppwStopReason.OFFSET_RECEIVED != stopFuture.getStopReason() || stopFuture.getFuture().failed()) {
            Throwable error = stopFuture.getFuture().failed() ? stopFuture.getFuture().cause() : stopFuture.getCause();
            return String.format(WITH_ERROR_REASON_TEMPLATE,
                    stopFuture.getSourceType().name(), stopFuture.getStopReason().name(),
                    stopFuture.getOffset() == null ? -1L : stopFuture.getOffset(),
                    error == null ? "" : NestedExceptionUtils.getMostSpecificCause(error).getMessage());
        } else {
            return String.format(OFFSET_RECEIVED_TEMPLATE,
                    stopFuture.getSourceType().name(), stopFuture.getStopReason().name(),
                    stopFuture.getOffset() == null ? -1L : stopFuture.getOffset());
        }
    }
}
