package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.schema;

import ru.ibs.dtm.common.model.ddl.ClassTypes;

/**
 * Конвертация физического типа в тип Tarantool
 */
public class SpaceAttributeTypeUtil {

  public static SpaceAttributeTypes toAttributeType(ClassTypes type) {
    switch (type) {
      case DATETIME:
      case TIMESTAMP:
      case DATE:
      case CHAR:
      case VARCHAR:
      case DECIMAL:
      case DEC:
      case NUMERIC:
      case FIXED:
      case ANY:
        return SpaceAttributeTypes.STRING;
      case INT:
      case INTEGER:
      case FLOAT:
      case DOUBLE:
      case BIGINT:
        return SpaceAttributeTypes.NUMBER;
      case BOOL:
      case BOOLEAN:
      case TINYINT:
        return SpaceAttributeTypes.BOOLEAN;
      default:
        throw new UnsupportedOperationException(String.format("Не поддержан тип: %s", type));
    }
  }
}
