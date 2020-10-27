package io.arenadata.dtm.query.execution.core.dto.delta.query;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.LocalDateTime;
import java.util.UUID;

import static io.arenadata.dtm.query.execution.core.dto.delta.query.DeltaAction.COMMIT_DELTA;

@Data
@EqualsAndHashCode(callSuper = true)
public class CommitDeltaQuery extends DeltaQuery {

    @Builder
    public CommitDeltaQuery(UUID requestId, String datamart, Long deltaNum, LocalDateTime deltaDate) {
        super(requestId, datamart, deltaNum, deltaDate);
    }

    @Override
    public DeltaAction getDeltaAction() {
        return COMMIT_DELTA;
    }
}
