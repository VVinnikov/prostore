package io.arenadata.dtm.common.reader;

import java.util.Arrays;

/**
 * Представление из информационной схемы
 */
public enum InformationSchemaView {
  SCHEMES("logic_schema_datamarts"),
  TABLES("logic_schema_entities"),
  DELTAS("logic_schema_deltas"),
  COLUMNS("logic_schema_columns"),
  TABLE_CONSTRAINTS("logic_schema_table_constraints"),
  KEY_COLUMN_USAGE("logic_schema_key_column_usage");

  public static final String SCHEMA_NAME = "INFORMATION_SCHEMA";
  private static final String quiet = "\"";
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
    return quiet + name + quiet;
  }

  private String getFullName(boolean schemaQuotes, boolean nameQuotes) {
    return (schemaQuotes ? addQuotes(SCHEMA_NAME) : SCHEMA_NAME) + "." +
      (nameQuotes ? addQuotes(name()) : name());
  }
}
