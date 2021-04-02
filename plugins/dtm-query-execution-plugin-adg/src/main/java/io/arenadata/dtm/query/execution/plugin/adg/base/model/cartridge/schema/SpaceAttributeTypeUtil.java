package io.arenadata.dtm.query.execution.plugin.adg.base.model.cartridge.schema;

import io.arenadata.dtm.common.model.ddl.ColumnType;

/**
 * Conversion from physical type to Tarantool type
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
                return SpaceAttributeTypes.NUMBER;
            case BOOLEAN:
                return SpaceAttributeTypes.BOOLEAN;
            default:
                throw new UnsupportedOperationException(String.format("Unsupported type: %s", type));
        }
    }
}
