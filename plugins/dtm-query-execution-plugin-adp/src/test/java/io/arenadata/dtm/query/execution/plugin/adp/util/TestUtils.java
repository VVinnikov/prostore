package io.arenadata.dtm.query.execution.plugin.adp.util;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import lombok.val;

import java.util.Arrays;
import java.util.UUID;

public class TestUtils {

    public static final String SCHEMA = "datamart";
    public static final String TABLE = "table";

    public static DdlRequest createDdlRequest() {
        return new DdlRequest(UUID.randomUUID(), "env", SCHEMA, new Entity(), null);
    }

    public static DdlRequest createDdlRequest(Entity entity) {
        return new DdlRequest(UUID.randomUUID(), "env", SCHEMA, entity, null);
    }

    public static Entity createAllTypesEntity() {
        val fields = Arrays.asList(
                createEntityField(0, "id", ColumnType.INT, null, 1, 1),
                createEntityField(1, "double_col", ColumnType.DOUBLE, null, null, null),
                createEntityField(2, "float_col", ColumnType.FLOAT, null, null, null),
                createEntityField(3, "varchar_col", ColumnType.VARCHAR, 36, null, null),
                createEntityField(4, "boolean_col", ColumnType.BOOLEAN, null, null, null),
                createEntityField(5, "int_col", ColumnType.INT, null, null, null),
                createEntityField(6, "bigint_col", ColumnType.BIGINT, null, null, null),
                createEntityField(9, "date_col", ColumnType.DATE, null, null, null),
                createEntityField(7, "timestamp_col", ColumnType.TIMESTAMP, 6, null, null),
                createEntityField(8, "time_col", ColumnType.TIME, 5, null, null),
                createEntityField(9, "uuid_col", ColumnType.UUID, 36, null, null),
                createEntityField(9, "char_col", ColumnType.CHAR, 10, null, null),
                createEntityField(9, "int32_col", ColumnType.INT32, null, null, null)
        );
        return Entity.builder()
                .schema(SCHEMA)
                .name(TABLE)
                .fields(fields)
                .entityType(EntityType.TABLE)
                .build();
    }

    private static EntityField createEntityField(int ordinalPosition, String name, ColumnType type, Integer size,
                                  Integer primaryOrder, Integer shardingOrder) {
        return EntityField.builder()
                .ordinalPosition(ordinalPosition)
                .name(name)
                .type(type)
                .size(size)
                .accuracy(null)
                .nullable(true)
                .primaryOrder(primaryOrder)
                .shardingOrder(shardingOrder)
                .defaultValue(null)
                .build();
    }
}
