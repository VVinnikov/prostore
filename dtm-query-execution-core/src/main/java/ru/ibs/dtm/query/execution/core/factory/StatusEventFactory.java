package ru.ibs.dtm.query.execution.core.factory;

import org.springframework.beans.factory.annotation.Autowired;
import ru.ibs.dtm.common.status.PublishStatusEventRequest;
import ru.ibs.dtm.common.status.StatusEventCode;
import ru.ibs.dtm.query.execution.core.registry.StatusEventFactoryRegistry;

public interface StatusEventFactory<OUT> {
    PublishStatusEventRequest<OUT> create(String datamart, String eventData);

    StatusEventCode getEventCode();

    @Autowired
    default void registry(StatusEventFactoryRegistry registry) {
        registry.registryFactory(this);
    }

}