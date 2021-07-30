package io.arenadata.dtm.query.execution.plugin.adp.base.dto.metadata;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AdpTableColumn {

    private final String name;
    private final String type;
    private final Boolean nullable;

}
