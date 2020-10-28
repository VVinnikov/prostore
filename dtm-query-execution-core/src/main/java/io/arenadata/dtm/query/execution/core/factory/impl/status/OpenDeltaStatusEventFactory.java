package io.arenadata.dtm.query.execution.core.factory.impl.status;

import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.common.status.StatusEventKey;
import io.arenadata.dtm.common.status.delta.OpenDeltaEvent;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaRecord;
import io.arenadata.dtm.query.execution.core.factory.AbstractStatusEventFactory;
import org.springframework.stereotype.Component;

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
        return new OpenDeltaEvent(eventData.getDeltaNum());
    }
}
