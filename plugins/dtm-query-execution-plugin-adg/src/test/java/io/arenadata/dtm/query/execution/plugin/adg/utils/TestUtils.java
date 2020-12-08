package io.arenadata.dtm.query.execution.plugin.adg.utils;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityFieldUtils;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.schema.*;

import java.util.*;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adg.constants.ColumnFields.*;
import static io.arenadata.dtm.query.execution.plugin.adg.constants.ColumnFields.SYS_OP_FIELD;
import static io.arenadata.dtm.query.execution.plugin.adg.factory.impl.AdgTableEntitiesFactory.SEC_INDEX_PREFIX;

public class TestUtils {
    public static final List<String> spacePostfixes = Arrays.asList(ACTUAL_POSTFIX, HISTORY_POSTFIX, STAGING_POSTFIX);

    public static Entity getEntity() {
        List<EntityField> keyFields = Arrays.asList(
                new EntityField(0, "id", ColumnType.INT.name(), false, 1, 1, null),
                new EntityField(1, "sk_key2", ColumnType.INT.name(), false, null, 2, null),
                new EntityField(2, "pk2", ColumnType.INT.name(), false, 2, null, null),
                new EntityField(3, "sk_key3", ColumnType.INT.name(), false, null, 3, null)
        );
        ColumnType[] types = ColumnType.values();
        List<EntityField> fields = new ArrayList<>();
        for (int i = 0; i < types.length; i++) {
            ColumnType type = types[i];
            if (Arrays.asList(ColumnType.BLOB, ColumnType.ANY).contains(type)) {
                continue;
            }

            EntityField.EntityFieldBuilder builder = EntityField.builder()
                    .ordinalPosition(i + keyFields.size())
                    .type(type)
                    .nullable(true)
                    .name(type.name() + "_type");
            if (Arrays.asList(ColumnType.CHAR, ColumnType.VARCHAR).contains(type)) {
                builder.size(20);
            } else if (Arrays.asList(ColumnType.TIME, ColumnType.TIMESTAMP).contains(type)) {
                builder.accuracy(5);
            }
            fields.add(builder.build());
        }
        fields.addAll(keyFields);
        return new Entity("test_schema.test_table", fields);
    }

    public static Map<String, Space> getSpaces(Entity entity) {
        Map<String, List<SpaceIndex>> spaceIndexMap = getSpaceIndexMap();
        List<SpaceAttribute> pkAttrs = EntityFieldUtils.getPrimaryKeyList(entity.getFields()).stream()
                .map(TestUtils::createAttribute)
                .collect(Collectors.toList());
        List<SpaceAttribute> logAttrs = entity.getFields().stream()
                .filter(field -> field.getPrimaryOrder() == null)
                .map(TestUtils::createAttribute)
                .collect(Collectors.toList());
        return spacePostfixes.stream()
                .collect(Collectors.toMap(
                        postfix -> String.format("env__%s__%s%s", entity.getSchema(), entity.getName(), postfix),
                        postfix -> Space.builder()
                                .format(getAttrs(postfix, pkAttrs, logAttrs))
                                .indexes(spaceIndexMap.get(postfix))
                                .build()));
    }

    public static Map<String, List<SpaceIndex>> getSpaceIndexMap() {
        Map<String, List<SpaceIndex>> spaceIndexMap = new HashMap<>();
        spaceIndexMap.put(ACTUAL_POSTFIX, Arrays.asList(
                new SpaceIndex(true, Collections.emptyList(), SpaceIndexTypes.TREE, ID),
                new SpaceIndex(false, Collections.emptyList(), SpaceIndexTypes.TREE, SEC_INDEX_PREFIX + SYS_FROM_FIELD),
                new SpaceIndex(false, Collections.emptyList(), SpaceIndexTypes.TREE, BUCKET_ID)
        ));
        spaceIndexMap.put(HISTORY_POSTFIX, Arrays.asList(
                new SpaceIndex(true, Collections.emptyList(), SpaceIndexTypes.TREE, ID),
                new SpaceIndex(false, Collections.emptyList(), SpaceIndexTypes.TREE, SEC_INDEX_PREFIX + SYS_FROM_FIELD),
                new SpaceIndex(false, Collections.emptyList(), SpaceIndexTypes.TREE, SEC_INDEX_PREFIX + SYS_TO_FIELD),
                new SpaceIndex(false, Collections.emptyList(), SpaceIndexTypes.TREE, BUCKET_ID)
        ));
        spaceIndexMap.put(STAGING_POSTFIX, Arrays.asList(
                new SpaceIndex(true, Collections.emptyList(), SpaceIndexTypes.TREE, ID),
                new SpaceIndex(false, Collections.emptyList(), SpaceIndexTypes.TREE, BUCKET_ID)
        ));
        return spaceIndexMap;
    }

    public static List<SpaceAttribute> getAttrs(String postfix,
                                                List<SpaceAttribute> pkAttrs,
                                                List<SpaceAttribute> logAttrs) {
        List<SpaceAttribute> result = new ArrayList<>(pkAttrs);
        result.add(new SpaceAttribute(false, BUCKET_ID, SpaceAttributeTypes.UNSIGNED));
        switch (postfix) {
            case ACTUAL_POSTFIX:
            case HISTORY_POSTFIX:
                result.add(new SpaceAttribute(false, SYS_FROM_FIELD, SpaceAttributeTypes.NUMBER));
                result.add(new SpaceAttribute(true, SYS_TO_FIELD, SpaceAttributeTypes.NUMBER));
                result.add(new SpaceAttribute(false, SYS_OP_FIELD, SpaceAttributeTypes.NUMBER));
                break;
            case STAGING_POSTFIX:
                result.add(new SpaceAttribute(false, SYS_OP_FIELD, SpaceAttributeTypes.NUMBER));
                break;
        }
        result.addAll(logAttrs);
        return result;
    }

    private static SpaceAttribute createAttribute(EntityField field) {
        return new SpaceAttribute(field.getNullable(), field.getName(),
                SpaceAttributeTypeUtil.toAttributeType(field.getType()));
    }
}