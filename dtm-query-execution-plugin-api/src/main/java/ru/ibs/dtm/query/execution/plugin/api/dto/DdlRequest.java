package ru.ibs.dtm.query.execution.plugin.api.dto;

import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.reader.QueryRequest;

/**
 * dto для выполнения ddl
 */
public class DdlRequest extends BaseRequest{

  /**
   * Модель таблицы в служебной БД
   */
  private ClassTable classTable;

  /**
   * Требуется создание топика
   */
  private boolean createTopic;

  public DdlRequest() {
  }

  public DdlRequest(QueryRequest queryRequest, ClassTable classTable, boolean createTopic) {
    super(queryRequest);
    this.classTable = classTable;
    this.createTopic = createTopic;
  }

  public ClassTable getClassTable() {
    return classTable;
  }

  public void setClassTable(ClassTable classTable) {
    this.classTable = classTable;
  }

  public boolean isCreateTopic() {
    return createTopic;
  }

  public void setCreateTopic(boolean createTopic) {
    this.createTopic = createTopic;
  }

  @Override
  public String toString() {
    return "DdlRequest{" +
      super.toString() +
      ", classTable=" + classTable +
      ", createTopic=" + createTopic +
      '}';
  }
}
