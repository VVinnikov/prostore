package ru.ibs.dtm.query.execution.core.factory.impl.status;

import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.status.StatusEventCode;
import ru.ibs.dtm.common.status.StatusEventKey;
import ru.ibs.dtm.common.status.delta.CloseDeltaEvent;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;
import ru.ibs.dtm.query.execution.core.factory.AbstractStatusEventFactory;

@Component
public class CloseDeltaStatusEventFactory extends AbstractStatusEventFactory<DeltaRecord, CloseDeltaEvent> {

    protected CloseDeltaStatusEventFactory() {
        super(DeltaRecord.class);
    }

    @Override
    public StatusEventCode getEventCode() {
        return StatusEventCode.DELTA_CLOSE;
    }

    @Override
    protected CloseDeltaEvent createEventMessage(StatusEventKey eventKey, DeltaRecord eventData) {
        return new CloseDeltaEvent(eventData.getSinId(), eventData.getStatusDate());
    }
}
