package io.arenadata.dtm.query.execution.plugin.adp.base.dto.metadata;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AdpTables<T> {

    private final T actual;
    private final T history;
    private final T staging;

}
