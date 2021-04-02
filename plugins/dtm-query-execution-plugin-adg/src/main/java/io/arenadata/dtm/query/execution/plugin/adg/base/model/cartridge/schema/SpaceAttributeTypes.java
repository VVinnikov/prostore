package io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.schema;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Tarantool types
 */
public enum SpaceAttributeTypes {
    UNSIGNED("unsigned"),
    NUMBER("number"),
    STRING("string"),
    INTEGER("integer"),
    DOUBLE("double"),
    BOOLEAN("boolean");

    private String name;

    SpaceAttributeTypes(String name) {
        this.name = name;
    }

    @JsonValue
    public String getName() {
        return name;
    }
}
