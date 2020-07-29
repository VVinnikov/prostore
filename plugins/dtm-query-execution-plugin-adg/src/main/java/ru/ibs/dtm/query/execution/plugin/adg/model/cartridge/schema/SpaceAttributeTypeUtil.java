package ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.schema;

import ru.ibs.dtm.common.model.ddl.ColumnType;

/**
 * Конвертация физического типа в тип Tarantool
 */
public class SpaceAttributeTypeUtil {

    public static SpaceAttributeTypes toAttributeType(ColumnType type) {
        switch (type) {
            case UUID:
            case CHAR:
            case VARCHAR:
            case ANY:
                return SpaceAttributeTypes.STRING;
            case DATE:
            case TIME:
            case TIMESTAMP:
            case INT:
            case BIGINT:
                return SpaceAttributeTypes.INTEGER;
            case FLOAT:
            case DOUBLE:
                return SpaceAttributeTypes.DOUBLE;
            case BOOLEAN:
                return SpaceAttributeTypes.BOOLEAN;
            default:
                throw new UnsupportedOperationException(String.format("Не поддержан тип: %s", type));
        }
    }
}
