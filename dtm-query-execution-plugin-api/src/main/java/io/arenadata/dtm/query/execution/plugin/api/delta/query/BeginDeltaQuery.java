package io.arenadata.dtm.query.execution.plugin.api.delta.query;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import static io.arenadata.dtm.query.execution.plugin.api.delta.query.DeltaAction.BEGIN_DELTA;

@EqualsAndHashCode(callSuper = true)
@Data
@ToString
public class BeginDeltaQuery extends DeltaQuery {

    private Long deltaNum;

    @Override
    public DeltaAction getDeltaAction() {
        return BEGIN_DELTA;
    }
}
