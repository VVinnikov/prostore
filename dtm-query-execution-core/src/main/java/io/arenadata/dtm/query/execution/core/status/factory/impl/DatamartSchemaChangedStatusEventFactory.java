package io.arenadata.dtm.query.execution.core.status.factory.impl;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.common.status.StatusEventKey;
import io.arenadata.dtm.common.status.ddl.DatamartSchemaChangedEvent;
import io.arenadata.dtm.query.execution.core.status.factory.AbstractStatusEventFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DatamartSchemaChangedStatusEventFactory extends AbstractStatusEventFactory<DatamartSchemaChangedEvent, DatamartSchemaChangedEvent> {

    @Autowired
    protected DatamartSchemaChangedStatusEventFactory(DtmConfig dtmSettings) {
        super(DatamartSchemaChangedEvent.class, dtmSettings);
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
