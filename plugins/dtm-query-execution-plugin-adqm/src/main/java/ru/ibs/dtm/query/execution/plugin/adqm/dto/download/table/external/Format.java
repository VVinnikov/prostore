package ru.ibs.dtm.query.execution.plugin.adqm.dto.download.table.external;

import com.fasterxml.jackson.annotation.JsonFormat;

@JsonFormat(shape = JsonFormat.Shape.NUMBER_INT)
public enum Format {
    AVRO("avro"),
    CSV("csv"),
    TEXT("text");

    private String name;

    Format(String name) {
        this.name = name;
    }

    public static Format findByName(String name) {
        for (Format value : Format.values()) {
            if (value.name.equalsIgnoreCase(name)) {
                return value;
            }
        }
        throw new IllegalArgumentException("Не найден соответствующий формат для " + name);
    }

    public String getName() {
        return name;
    }
}
