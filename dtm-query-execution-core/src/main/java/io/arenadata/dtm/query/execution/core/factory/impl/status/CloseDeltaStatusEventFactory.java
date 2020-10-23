package io.arenadata.dtm.query.execution.core.factory.impl.status;

import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.common.status.StatusEventKey;
import io.arenadata.dtm.common.status.delta.CloseDeltaEvent;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaRecord;
import io.arenadata.dtm.query.execution.core.factory.AbstractStatusEventFactory;
import org.springframework.stereotype.Component;

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
