package io.arenadata.dtm.query.execution.plugin.adg.service.impl.ddl;

import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.schema.AdgSpace;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AdgCreateTableQueries {
    private final AdgSpace actualTableSpace;
    private final AdgSpace historyTableSpace;
    private final AdgSpace stagingTableSpace;
}
