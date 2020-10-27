package io.arenadata.dtm.query.execution.core.dto.delta.query;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.time.LocalDateTime;
import java.util.UUID;

import static io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaAction.BEGIN_DELTA;

@EqualsAndHashCode(callSuper = true)
@Data
public class BeginDeltaQuery extends DeltaQuery {

    @Builder
    public BeginDeltaQuery(UUID requestId, String datamart, Long deltaNum, LocalDateTime deltaDate) {
        super(requestId, datamart, deltaNum, deltaDate);
    }

    @Override
    public DeltaAction getDeltaAction() {
        return BEGIN_DELTA;
    }
}
