package ru.ibs.dtm.query.execution.core.registry.impl;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.status.StatusEventCode;
import ru.ibs.dtm.query.execution.core.factory.StatusEventFactory;
import ru.ibs.dtm.query.execution.core.registry.StatusEventFactoryRegistry;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class StatusEventFactoryRegistryImpl implements StatusEventFactoryRegistry {
    private final Map<StatusEventCode, StatusEventFactory<?>> factoryMap;

    public StatusEventFactoryRegistryImpl() {
        factoryMap = new HashMap<>();
    }

    @Override
    public StatusEventFactory<?> get(StatusEventCode eventCode) {
        if (factoryMap.containsKey(eventCode)) {
            return factoryMap.get(eventCode);
        } else {
            throw new RuntimeException("StatusEventCode not supported: " + eventCode);
        }
    }

    @Override
    public void registryFactory(StatusEventFactory<?> factory) {
        factoryMap.put(factory.getEventCode(), factory);
    }
}
