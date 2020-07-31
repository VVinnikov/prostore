package ru.ibs.dtm.query.execution.core.factory.impl.status;

import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.status.StatusEventCode;
import ru.ibs.dtm.common.status.StatusEventKey;
import ru.ibs.dtm.common.status.delta.OpenDeltaEvent;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;
import ru.ibs.dtm.query.execution.core.factory.AbstractStatusEventFactory;

@Component
public class OpenDeltaStatusEventFactory extends AbstractStatusEventFactory<DeltaRecord, OpenDeltaEvent> {

    protected OpenDeltaStatusEventFactory() {
        super(DeltaRecord.class);
    }

    @Override
    public StatusEventCode getEventCode() {
        return StatusEventCode.DELTA_OPEN;
    }

    @Override
    protected OpenDeltaEvent createEventMessage(StatusEventKey eventKey, DeltaRecord eventData) {
        return new OpenDeltaEvent(eventData.getSinId());
    }
}
