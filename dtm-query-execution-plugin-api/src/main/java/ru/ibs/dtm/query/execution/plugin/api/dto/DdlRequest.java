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

  public DdlRequest() {
  }

  public DdlRequest(QueryRequest queryRequest, ClassTable classTable) {
    super(queryRequest);
    this.classTable = classTable;
  }

  public ClassTable getClassTable() {
    return classTable;
  }

  public void setClassTable(ClassTable classTable) {
    this.classTable = classTable;
  }

  @Override
  public String toString() {
    return "DdlRequest{" +
      super.toString() +
      ", classTable=" + classTable +
      '}';
  }
}
