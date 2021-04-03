package io.arenadata.dtm.query.execution.plugin.adg.base.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AdgTables<T> {
    private final T actual;
    private final T history;
    private final T staging;
}
