package ru.ibs.dtm.query.execution.plugin.adg.model.metadata;

public class ColumnTypeUtil {

  public static ColumnType columnTypeFromTtColumnType(String columnType) {
    switch (columnType) {
      case "unsigned":
        return ColumnType.LONG;
      case "number":
        return ColumnType.BIG_DECIMAL;
      case "boolean":
        return ColumnType.BOOLEAN;
      case "string":
        return ColumnType.STRING;
      default:
        return ColumnType.ANY;
    }
  }
}
