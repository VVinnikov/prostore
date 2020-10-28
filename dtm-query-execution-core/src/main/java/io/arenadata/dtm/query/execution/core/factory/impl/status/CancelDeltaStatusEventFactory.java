package io.arenadata.dtm.query.execution.core.factory.impl.status;

import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.common.status.StatusEventKey;
import io.arenadata.dtm.common.status.delta.CancelDeltaEvent;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaRecord;
import io.arenadata.dtm.query.execution.core.factory.AbstractStatusEventFactory;
import org.springframework.stereotype.Component;

@Component
public class CancelDeltaStatusEventFactory extends AbstractStatusEventFactory<DeltaRecord, CancelDeltaEvent> {

    protected CancelDeltaStatusEventFactory() {
        super(DeltaRecord.class);
    }

    @Override
    public StatusEventCode getEventCode() {
        return StatusEventCode.DELTA_CANCEL;
    }

    @Override
    protected CancelDeltaEvent createEventMessage(StatusEventKey eventKey, DeltaRecord eventData) {
        return new CancelDeltaEvent(eventData.getSinId());
    }
}
