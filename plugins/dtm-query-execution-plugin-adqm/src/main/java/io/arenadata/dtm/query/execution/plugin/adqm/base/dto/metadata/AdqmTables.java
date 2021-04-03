package io.arenadata.dtm.query.execution.plugin.adqm.base.dto.metadata;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AdqmTables<T> {
    private final T shard;
    private final T distributed;
}
