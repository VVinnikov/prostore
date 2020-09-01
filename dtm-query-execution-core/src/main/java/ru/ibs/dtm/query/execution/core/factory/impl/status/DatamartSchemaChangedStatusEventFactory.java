package ru.ibs.dtm.query.execution.core.factory.impl.status;

import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.status.StatusEventCode;
import ru.ibs.dtm.common.status.StatusEventKey;
import ru.ibs.dtm.common.status.ddl.DatamartSchemaChangedEvent;
import ru.ibs.dtm.query.execution.core.factory.AbstractStatusEventFactory;

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
