package io.arenadata.dtm.common.reader;

import java.util.Arrays;

/**
 * Information schema views
 */
public enum InformationSchemaView {
    SCHEMATA("logic_schema_datamarts"),
    TABLES("logic_schema_entities"),
    COLUMNS("logic_schema_columns"),
    TABLE_CONSTRAINTS("logic_schema_entity_constraints"),
    KEY_COLUMN_USAGE("logic_schema_key_column_usage");

    public static final String SCHEMA_NAME = "INFORMATION_SCHEMA";
    public static final String DTM_SCHEMA_NAME = "DTM";
    private static final String DOUBLE_QUOTE = "\"";
    private final String realName;

    InformationSchemaView(String realName) {
        this.realName = realName;
    }

    public String getRealName() {
        return realName;
    }

    public String getFullName() {
        return getFullName(false, false);
    }

    public static InformationSchemaView findByFullName(String fullName) {
        return Arrays.stream(InformationSchemaView.values())
                .filter(view -> view.equalsByFullName(fullName))
                .findAny().orElse(null);
    }

    private boolean equalsByFullName(String fullName) {
        boolean[] boolValues = {false, true};
        for (boolean schemaQuotes : boolValues) {
            for (boolean nameQuotes : boolValues) {
                if (getFullName(schemaQuotes, nameQuotes).equalsIgnoreCase(fullName)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static String addQuotes(String name) {
        return DOUBLE_QUOTE + name + DOUBLE_QUOTE;
    }

    private String getFullName(boolean schemaQuotes, boolean nameQuotes) {
        return (schemaQuotes ? addQuotes(SCHEMA_NAME) : SCHEMA_NAME) + "." +
                (nameQuotes ? addQuotes(name()) : name());
    }
}
