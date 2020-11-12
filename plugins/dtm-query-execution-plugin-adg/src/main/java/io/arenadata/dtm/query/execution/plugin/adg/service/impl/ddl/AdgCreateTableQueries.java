package io.arenadata.dtm.query.execution.plugin.adg.service.impl.ddl;

import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.schema.AdgSpace;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AdgCreateTableQueries {
    private final AdgSpace actual;
    private final AdgSpace history;
    private final AdgSpace staging;
}
