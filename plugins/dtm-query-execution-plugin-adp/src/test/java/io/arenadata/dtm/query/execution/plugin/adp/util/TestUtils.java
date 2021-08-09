package io.arenadata.dtm.query.execution.plugin.adp.util;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.query.execution.plugin.adp.base.dto.metadata.AdpTableColumn;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import lombok.val;

import java.util.*;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adp.base.utils.AdpTypeUtil.adpTypeFromDtmType;

public class TestUtils {

    public static final String SCHEMA = "datamart";
    public static final String TABLE = "table";

    public static DdlRequest createDdlRequest() {
        return new DdlRequest(UUID.randomUUID(), "env", SCHEMA, new Entity(), null);
    }

    public static DdlRequest createDdlRequest(Entity entity) {
        return new DdlRequest(UUID.randomUUID(), "env", SCHEMA, entity, null);
    }

    public static Entity createAllTypesTable() {
        List<ColumnType> allTypes = Arrays.stream(ColumnType.values())
                .filter(type -> !type.equals(ColumnType.BLOB) && !type.equals(ColumnType.ANY))
                .collect(Collectors.toList());
        List<ColumnType> sizedTimes = Arrays.asList(ColumnType.TIMESTAMP, ColumnType.TIME);
        List<ColumnType> sizedChars = Arrays.asList(ColumnType.VARCHAR, ColumnType.CHAR);
        List<EntityField> fields = new ArrayList<>();
        fields.add(createEntityField(0, "id", ColumnType.INT, null, false, 1, 1));
        for (int i = 0; i < allTypes.size(); i++) {
            val columnType = allTypes.get(i);
            Integer size = null;
            if (sizedTimes.contains(columnType)) {
                size = 6;
            }
            if (sizedChars.contains(columnType)) {
                size = 10;
            }
            if (columnType.equals(ColumnType.UUID)) {
                size = 36;
            }
            fields.add(createEntityField(
                    i + 1,
                    columnType.name().toLowerCase() + "_col",
                    columnType,
                    size));
        }
        return Entity.builder()
                .schema(SCHEMA)
                .name(TABLE)
                .fields(fields)
                .entityType(EntityType.TABLE)
                .build();
    }

    public static List<AdpTableColumn> adpTableColumnsFromEntityFields(List<EntityField> fields) {
        return fields.stream()
                .map(field -> new AdpTableColumn(field.getName(), adpTypeFromDtmType(field.getType(), field.getSize()), field.getNullable()))
                .collect(Collectors.toList());
    }

    private static EntityField createEntityField(int ordinalPosition, String name, ColumnType type, Integer size,
                                  boolean nullable, Integer primaryOrder, Integer shardingOrder) {
        return EntityField.builder()
                .ordinalPosition(ordinalPosition)
                .name(name)
                .type(type)
                .size(size)
                .accuracy(null)
                .nullable(nullable)
                .primaryOrder(primaryOrder)
                .shardingOrder(shardingOrder)
                .defaultValue(null)
                .build();
    }

    private static EntityField createEntityField(int ordinalPosition, String name, ColumnType type, Integer size) {
        return createEntityField(ordinalPosition, name, type, size, true, null, null);
    }
}
