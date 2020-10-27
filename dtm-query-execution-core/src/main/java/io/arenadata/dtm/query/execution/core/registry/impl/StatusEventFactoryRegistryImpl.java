package io.arenadata.dtm.query.execution.core.registry.impl;

import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.query.execution.core.factory.StatusEventFactory;
import io.arenadata.dtm.query.execution.core.registry.StatusEventFactoryRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

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