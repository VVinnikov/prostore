package io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.schema;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Space engines
 */
public enum SpaceEngines {
    MEMTX("memtx"), VINYL("vinyl");

    private String name;

    SpaceEngines(String name) {
        this.name = name;
    }

    @JsonValue
    public String getName() {
        return name;
    }
}
