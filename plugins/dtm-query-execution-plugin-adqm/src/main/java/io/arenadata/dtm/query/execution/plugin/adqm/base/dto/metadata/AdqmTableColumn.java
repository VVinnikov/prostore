package io.arenadata.dtm.query.execution.plugin.adqm.base.dto.metadata;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
@AllArgsConstructor
public class AdqmTableColumn {
    private final String name;
    private final String type;
    private final Boolean nullable;
}
