package io.arenadata.dtm.query.execution.plugin.adg.factory.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityFieldUtils;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.adg.configuration.properties.TarantoolDatabaseProperties;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.schema.*;
import io.arenadata.dtm.query.execution.plugin.adg.service.impl.ddl.AdgCreateTableQueries;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.CreateTableQueriesFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adg.constants.ColumnFields.*;

@Component
public class AdgCreateTableQueriesFactory implements CreateTableQueriesFactory<AdgCreateTableQueries> {

    public static final String SEC_INDEX_PREFIX = "x_";
    public static final String TABLE_NAME_DELIMITER = "__";
    private final SpaceEngines engine;

    @Autowired
    public AdgCreateTableQueriesFactory(TarantoolDatabaseProperties tarantoolProperties) {
        this.engine = SpaceEngines.valueOf(tarantoolProperties.getEngine());
    }

    @Override
    public AdgCreateTableQueries create(DdlRequestContext context) {
        QueryRequest queryRequest = context.getRequest().getQueryRequest();
        String prefix = queryRequest.getEnvName() + TABLE_NAME_DELIMITER +
                queryRequest.getDatamartMnemonic() + TABLE_NAME_DELIMITER;
        Entity entity = context.getRequest().getEntity();
        int indexComma = entity.getName().indexOf(".");
        String table = entity.getName().substring(indexComma + 1).toLowerCase();

        return new AdgCreateTableQueries(
                new AdgSpace(prefix + table + ACTUAL_POSTFIX,
                        create(entity.getFields(), engine, ACTUAL_POSTFIX)),
                new AdgSpace(prefix + table + HISTORY_POSTFIX,
                        create(entity.getFields(), engine, HISTORY_POSTFIX)),
                new AdgSpace(prefix + table + STAGING_POSTFIX,
                        createStagingSpace(entity.getFields(), engine))
        );
    }

    private Space create(List<EntityField> fields,
                         SpaceEngines engine,
                         String tablePostfix) {
        List<SpaceIndexPart> primaryKeyParts = getPrimaryKeyParts(fields);
        primaryKeyParts.add(new SpaceIndexPart(SYS_FROM_FIELD, SpaceAttributeTypes.NUMBER.getName(), false));
        return new Space(
                getAttributes(fields),
                false,
                engine,
                false,
                getShardingKey(fields),
                createSpaceIndexes(fields, tablePostfix));
    }

    private List<SpaceIndex> createSpaceIndexes(List<EntityField> fields,
                                                String tablePosfix) {
        switch (tablePosfix) {
            case ACTUAL_POSTFIX:
                return Arrays.asList(
                        new SpaceIndex(true, getPrimaryKeyPartsWithSysFrom(fields), SpaceIndexTypes.TREE, ID),
                        new SpaceIndex(false, Collections.singletonList(
                                new SpaceIndexPart(SYS_FROM_FIELD, SpaceAttributeTypes.NUMBER.getName(), false)
                        ), SpaceIndexTypes.TREE, SEC_INDEX_PREFIX + SYS_FROM_FIELD),
                        new SpaceIndex(false, Collections.singletonList(
                                new SpaceIndexPart(BUCKET_ID, SpaceAttributeTypes.UNSIGNED.getName(), false)
                        ), SpaceIndexTypes.TREE, BUCKET_ID)
                );
            case HISTORY_POSTFIX:
                return Arrays.asList(
                        new SpaceIndex(true, getPrimaryKeyPartsWithSysFrom(fields), SpaceIndexTypes.TREE, ID),
                        new SpaceIndex(false, Collections.singletonList(
                                new SpaceIndexPart(SYS_FROM_FIELD, SpaceAttributeTypes.NUMBER.getName(), false)
                        ), SpaceIndexTypes.TREE, SEC_INDEX_PREFIX + SYS_FROM_FIELD),
                        new SpaceIndex(false, Arrays.asList(
                                new SpaceIndexPart(SYS_TO_FIELD, SpaceAttributeTypes.NUMBER.getName(), true),
                                new SpaceIndexPart(SYS_OP_FIELD, SpaceAttributeTypes.NUMBER.getName(), false)
                        ), SpaceIndexTypes.TREE, SEC_INDEX_PREFIX + SYS_TO_FIELD),
                        new SpaceIndex(false, Collections.singletonList(
                                new SpaceIndexPart(BUCKET_ID, SpaceAttributeTypes.UNSIGNED.getName(), false)
                        ), SpaceIndexTypes.TREE, BUCKET_ID)
                );
            case STAGING_POSTFIX:
                return Arrays.asList(
                        new SpaceIndex(true, getPrimaryKeyParts(fields), SpaceIndexTypes.TREE, ID),
                        new SpaceIndex(false, Collections.singletonList(
                                new SpaceIndexPart(BUCKET_ID, SpaceAttributeTypes.UNSIGNED.getName(), false)
                        ), SpaceIndexTypes.TREE, BUCKET_ID)
                );
            default:
                throw new RuntimeException(String.format("Table type [%s] doesn't support", tablePosfix));
        }
    }

    private List<SpaceIndexPart> getPrimaryKeyPartsWithSysFrom(List<EntityField> fields) {
        final List<SpaceIndexPart> spaceIndexParts = EntityFieldUtils.getPrimaryKeyList(fields).stream()
                .map(f -> new SpaceIndexPart(f.getName(),
                        SpaceAttributeTypeUtil.toAttributeType(f.getType()).getName(),
                        f.getNullable()))
                .collect(Collectors.toList());
        spaceIndexParts.add(new SpaceIndexPart(SYS_FROM_FIELD, SpaceAttributeTypes.NUMBER.getName(), false));
        return spaceIndexParts;
    }

    private List<SpaceIndexPart> getPrimaryKeyParts(List<EntityField> fields) {
        return EntityFieldUtils.getPrimaryKeyList(fields).stream()
                .map(f -> new SpaceIndexPart(f.getName(),
                        SpaceAttributeTypeUtil.toAttributeType(f.getType()).getName(),
                        f.getNullable()))
                .collect(Collectors.toList());
    }

    private Space createStagingSpace(List<EntityField> fields,
                                     SpaceEngines engine) {
        return new Space(
                getStagingAttributes(fields),
                false,
                engine,
                false,
                getShardingKey(fields),
                createSpaceIndexes(fields, STAGING_POSTFIX));
    }

    private List<String> getShardingKey(List<EntityField> fields) {
        List<String> sk = EntityFieldUtils.getShardingKeyList(fields).stream()
                .map(EntityField::getName)
                .collect(Collectors.toList());
        if (sk.size() == 0) {
            sk = getPrimaryKey(fields);
        }
        return sk;
    }

    private List<String> getPrimaryKey(List<EntityField> fields) {
        List<String> sk = EntityFieldUtils.getPrimaryKeyList(fields).stream()
                .map(EntityField::getName)
                .collect(Collectors.toList());
        if (sk.size() == 0) {
            sk = Collections.singletonList(ID);
        }
        return sk;
    }

    //The order is synchronized with the AVRO scheme
    private List<SpaceAttribute> getAttributes(List<EntityField> fields) {
        List<SpaceAttribute> attributes = fields.stream()
                .map(this::toAttribute)
                .collect(Collectors.toList());
        attributes.addAll(
                Arrays.asList(
                        new SpaceAttribute(false, SYS_OP_FIELD, SpaceAttributeTypes.NUMBER),
                        new SpaceAttribute(false, SYS_FROM_FIELD, SpaceAttributeTypes.NUMBER),
                        new SpaceAttribute(true, SYS_TO_FIELD, SpaceAttributeTypes.NUMBER),
                        new SpaceAttribute(false, BUCKET_ID, SpaceAttributeTypes.UNSIGNED))
        );
        return attributes;
    }

    private List<SpaceAttribute> getStagingAttributes(List<EntityField> fields) {
        List<SpaceAttribute> attributes = fields.stream()
                .map(this::toAttribute)
                .collect(Collectors.toList());
        attributes.addAll(
                Arrays.asList(
                        new SpaceAttribute(false, SYS_OP_FIELD, SpaceAttributeTypes.NUMBER),
                        new SpaceAttribute(false, BUCKET_ID, SpaceAttributeTypes.UNSIGNED))
        );
        return attributes;
    }

    private SpaceAttribute toAttribute(EntityField field) {
        return new SpaceAttribute(field.getNullable(), field.getName(),
                SpaceAttributeTypeUtil.toAttributeType(field.getType()));
    }
}
