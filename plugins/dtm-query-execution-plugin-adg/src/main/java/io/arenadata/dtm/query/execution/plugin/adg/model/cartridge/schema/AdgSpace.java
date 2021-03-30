package io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.schema;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class AdgSpace {
    private final String name;
    private final Space space;
}
