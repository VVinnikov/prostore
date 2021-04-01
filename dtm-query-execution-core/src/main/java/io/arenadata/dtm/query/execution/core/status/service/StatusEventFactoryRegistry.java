package io.arenadata.dtm.query.execution.core.status.service;

import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.query.execution.core.status.factory.StatusEventFactory;

public interface StatusEventFactoryRegistry {
    StatusEventFactory<?> get(StatusEventCode eventCode);

    void registryFactory(StatusEventFactory<?> factory);
}
