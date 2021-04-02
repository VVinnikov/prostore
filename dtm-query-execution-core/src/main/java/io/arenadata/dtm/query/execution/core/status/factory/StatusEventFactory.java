package io.arenadata.dtm.query.execution.core.status.factory;

import io.arenadata.dtm.common.status.PublishStatusEventRequest;
import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.query.execution.core.status.service.StatusEventFactoryRegistry;
import org.springframework.beans.factory.annotation.Autowired;

public interface StatusEventFactory<OUT> {
    PublishStatusEventRequest<OUT> create(String datamart, String eventData);

    StatusEventCode getEventCode();

    @Autowired
    default void registry(StatusEventFactoryRegistry registry) {
        registry.registryFactory(this);
    }

}
