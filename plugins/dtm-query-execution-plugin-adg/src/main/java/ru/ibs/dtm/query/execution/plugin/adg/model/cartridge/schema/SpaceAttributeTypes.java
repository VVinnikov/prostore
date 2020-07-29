package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.schema;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Типы Tarantool
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
