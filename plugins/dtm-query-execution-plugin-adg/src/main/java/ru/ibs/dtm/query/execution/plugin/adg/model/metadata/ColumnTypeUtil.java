package ru.ibs.dtm.query.execution.plugin.adg.model.metadata;

import ru.ibs.dtm.query.execution.model.metadata.ColumnType;

public class ColumnTypeUtil {

  public static ColumnType columnTypeFromTtColumnType(String columnType) {
    switch (columnType) {
      case "unsigned":
        return ColumnType.BIGINT;
      case "number":
        return ColumnType.DOUBLE;
      case "boolean":
        return ColumnType.BOOLEAN;
      case "string":
        return ColumnType.VARCHAR;
      default:
        return ColumnType.ANY;
    }
  }
}
