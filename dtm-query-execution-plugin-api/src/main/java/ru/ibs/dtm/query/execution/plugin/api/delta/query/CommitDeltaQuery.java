package ru.ibs.dtm.query.execution.plugin.api.delta.query;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.LocalDateTime;

import static ru.ibs.dtm.query.execution.plugin.api.delta.query.DeltaAction.COMMIT_DELTA;

@Data
@EqualsAndHashCode(callSuper = false)
public class CommitDeltaQuery extends DeltaQuery {

    private LocalDateTime deltaDateTime;

    @Override
    public DeltaAction getDeltaAction() {
        return COMMIT_DELTA;
    }
}
