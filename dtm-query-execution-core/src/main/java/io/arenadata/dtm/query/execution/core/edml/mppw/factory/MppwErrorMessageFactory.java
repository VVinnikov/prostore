package io.arenadata.dtm.query.execution.core.edml.mppw.factory;

import io.arenadata.dtm.query.execution.core.edml.mppw.dto.MppwStopFuture;

public interface MppwErrorMessageFactory {
    String create(MppwStopFuture stopFuture);
}
