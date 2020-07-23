package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.schema;

import ru.ibs.dtm.common.model.ddl.ClassTypes;

/**
 * Конвертация физического типа в тип Tarantool
 */
public class SpaceAttributeTypeUtil {

  public static SpaceAttributeTypes toAttributeType(ClassTypes type) {
    switch (type) {
      case TIMESTAMP:
      case DATE:
      case CHAR:
      case VARCHAR:
      case ANY:
        return SpaceAttributeTypes.STRING;
      case INT:
      case FLOAT:
      case DOUBLE:
      case BIGINT:
        return SpaceAttributeTypes.NUMBER;
      case BOOLEAN:
        return SpaceAttributeTypes.BOOLEAN;
      default:
        throw new UnsupportedOperationException(String.format("Не поддержан тип: %s", type));
    }
  }
}
