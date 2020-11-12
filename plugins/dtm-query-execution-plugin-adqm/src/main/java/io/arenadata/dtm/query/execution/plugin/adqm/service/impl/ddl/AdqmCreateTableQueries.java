package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.ddl;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AdqmCreateTableQueries {
    private final String shard;
    private final String distributed;
}
