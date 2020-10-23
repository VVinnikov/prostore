package io.arenadata.dtm.query.execution.plugin.api.delta.query;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public abstract class DeltaQuery {

    public abstract DeltaAction getDeltaAction();
}
