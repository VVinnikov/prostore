package io.arenadata.dtm.query.execution.core.registry;

import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.query.execution.core.factory.StatusEventFactory;

public interface StatusEventFactoryRegistry {
    StatusEventFactory<?> get(StatusEventCode eventCode);

    void registryFactory(StatusEventFactory<?> factory);
}
