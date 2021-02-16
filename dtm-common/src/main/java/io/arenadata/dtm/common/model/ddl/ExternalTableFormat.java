package io.arenadata.dtm.common.model.ddl;

public enum ExternalTableFormat {
    AVRO("avro"),
    CSV("csv"),
    TEXT("text");

    private String name;

    ExternalTableFormat(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static ExternalTableFormat findByName(String name) {
        for (ExternalTableFormat value : ExternalTableFormat.values()) {
            if (value.name.equalsIgnoreCase(name)) {
                return value;
            }
        }
        throw new IllegalArgumentException("Cannot find corresponding format for " + name);
    }

}
