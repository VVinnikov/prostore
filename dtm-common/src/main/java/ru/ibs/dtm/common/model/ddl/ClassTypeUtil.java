package ru.ibs.dtm.common.model.ddl;

public class ClassTypeUtil {
  public static String pgFromMariaType(String columnType) {
    switch (columnType.toLowerCase()) {
      case "date": return "date";
      case "bigint": return "bigint";
      case "datetime":
      case "timestamp":
        return "timestamp";
      case "decimal":
      case "dec": return "decimal";
      case "numeric": return "numeric";
      case "float": return "float";
      case "double": return "double precision";
      case "boolean":
      case "bool":
        return "boolean";
      case "int": return "integer";
      case "char": return "char";
      case "varchar": return "varchar";
      default: throw new UnsupportedOperationException(String.format("Не поддержан тип: %s", columnType));
    }
  }
}
