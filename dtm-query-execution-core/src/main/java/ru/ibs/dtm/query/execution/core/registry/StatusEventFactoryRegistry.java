package ru.ibs.dtm.query.execution.core.registry;

import ru.ibs.dtm.common.status.StatusEventCode;
import ru.ibs.dtm.query.execution.core.factory.StatusEventFactory;

public interface StatusEventFactoryRegistry {
    StatusEventFactory<?> get(StatusEventCode eventCode);

    void registryFactory(StatusEventFactory<?> factory);
}
