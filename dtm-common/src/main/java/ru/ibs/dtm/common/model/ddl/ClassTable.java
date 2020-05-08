package ru.ibs.dtm.common.model.ddl;

import java.util.List;

/**
 * Физическая модель таблицы служебной БД
 */
public class ClassTable {

  private static final String DEFAULT_SCHEMA = "test";

  private String name;
  private String schema;
  private String nameWithSchema;
  private List<ClassField> fields;

  public ClassTable(String nameWithSchema, List<ClassField> fields) {
    this.nameWithSchema = nameWithSchema;
    this.fields = fields;
    parseNameWithSchema(nameWithSchema);
  }

  public ClassTable(String name, String schema, List<ClassField> fields) {
    this.name = name;
    this.schema = schema;
    this.fields = fields;
  }

  private void parseNameWithSchema(String nameWithSchema) {
    int indexComma = nameWithSchema.indexOf(".");
    this.schema = indexComma != -1 ? nameWithSchema.substring(0, indexComma) : DEFAULT_SCHEMA;
    this.name = nameWithSchema.substring(indexComma + 1);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public String getNameWithSchema() {
    return nameWithSchema;
  }

  public void setNameWithSchema(String nameWithSchema) {
    this.nameWithSchema = nameWithSchema;
  }

  public List<ClassField> getFields() {
    return fields;
  }

  public void setFields(List<ClassField> fields) {
    this.fields = fields;
  }
}

