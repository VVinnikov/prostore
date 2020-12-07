package io.arenadata.dtm.query.execution.plugin.adqm.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AdqmTables<T> {
    private final T shard;
    private final T distributed;
}
