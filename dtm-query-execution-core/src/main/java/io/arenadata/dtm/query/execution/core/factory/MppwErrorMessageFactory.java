package io.arenadata.dtm.query.execution.core.factory;

import io.arenadata.dtm.query.execution.core.dto.edml.MppwStopFuture;

public interface MppwErrorMessageFactory {
    String create(MppwStopFuture stopFuture);
}
