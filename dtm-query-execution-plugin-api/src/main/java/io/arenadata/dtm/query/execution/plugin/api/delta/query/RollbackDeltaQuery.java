package io.arenadata.dtm.query.execution.plugin.api.delta.query;

import lombok.Data;
import lombok.EqualsAndHashCode;

import static io.arenadata.dtm.query.execution.plugin.api.delta.query.DeltaAction.ROLLBACK_DELTA;

@Data
@EqualsAndHashCode(callSuper = false)
public class RollbackDeltaQuery extends DeltaQuery {
    @Override
    public DeltaAction getDeltaAction() {
        return ROLLBACK_DELTA;
    }
}
