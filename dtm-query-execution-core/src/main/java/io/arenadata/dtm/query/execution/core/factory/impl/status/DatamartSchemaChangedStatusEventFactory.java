package io.arenadata.dtm.query.execution.core.factory.impl.status;

import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.common.status.StatusEventKey;
import io.arenadata.dtm.common.status.ddl.DatamartSchemaChangedEvent;
import io.arenadata.dtm.query.execution.core.factory.AbstractStatusEventFactory;
import org.springframework.stereotype.Component;

@Component
public class DatamartSchemaChangedStatusEventFactory extends AbstractStatusEventFactory<DatamartSchemaChangedEvent, DatamartSchemaChangedEvent> {

    protected DatamartSchemaChangedStatusEventFactory() {
        super(DatamartSchemaChangedEvent.class);
    }

    @Override
    public StatusEventCode getEventCode() {
        return StatusEventCode.DATAMART_SCHEMA_CHANGED;
    }

    @Override
    protected DatamartSchemaChangedEvent createEventMessage(StatusEventKey eventKey, DatamartSchemaChangedEvent eventData) {
        return eventData;
    }
}
