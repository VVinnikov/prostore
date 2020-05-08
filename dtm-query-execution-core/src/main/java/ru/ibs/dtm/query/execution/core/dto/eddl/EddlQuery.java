package ru.ibs.dtm.query.execution.core.dto.eddl;

/**
 * Запрос eddl
 */
public abstract class EddlQuery {

  /**
   * Тип запроса
   */
  private EddlAction action;

  /**
   * Наименование схемы
   */
  private String schemaName;

  /**
   * Наименование таблицы
   */
  private String tableName;

  public EddlQuery(EddlAction action) {
    this.action = action;
  }

  public EddlQuery(EddlAction action, String schemaName, String tableName) {
    this(action);
    this.schemaName = schemaName;
    this.tableName = tableName;
  }

  public EddlAction getAction() {
    return action;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }
}
