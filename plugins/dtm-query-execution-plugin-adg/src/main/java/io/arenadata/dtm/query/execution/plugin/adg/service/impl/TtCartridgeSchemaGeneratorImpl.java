package io.arenadata.dtm.query.execution.plugin.adg.service.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityFieldUtils;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.plugin.adg.configuration.TarantoolDatabaseProperties;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.OperationYaml;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.schema.*;
import io.arenadata.dtm.query.execution.plugin.adg.service.TtCartridgeSchemaGenerator;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adg.constants.ColumnFields.*;

@Service
public class TtCartridgeSchemaGeneratorImpl implements TtCartridgeSchemaGenerator {

    public static final String SEC_INDEX_PREFIX = "x_";
    public static final String TABLE_NAME_DELIMITER = "__";
    private final SpaceEngines engine;

    @Autowired
    public TtCartridgeSchemaGeneratorImpl(TarantoolDatabaseProperties tarantoolProperties) {
        this.engine = SpaceEngines.valueOf(tarantoolProperties.getEngine());
    }

    @Override
    public void generate(DdlRequestContext context, OperationYaml yaml, Handler<AsyncResult<OperationYaml>> handler) {
        if (yaml.getSpaces() == null) {
            yaml.setSpaces(new LinkedHashMap<>());
        }
        val spaces = yaml.getSpaces();
        QueryRequest queryRequest = context.getRequest().getQueryRequest();
        String prefix = queryRequest.getEnvName() + TABLE_NAME_DELIMITER +
                queryRequest.getDatamartMnemonic() + TABLE_NAME_DELIMITER;
        Entity entity = context.getRequest().getEntity();
        int indexComma = entity.getName().indexOf(".");
        String table = entity.getName().substring(indexComma + 1).toLowerCase();

        spaces.put(prefix + table + ACTUAL_POSTFIX, create(entity.getFields(), engine, ACTUAL_POSTFIX));
        spaces.put(prefix + table + STAGING_POSTFIX, createStagingSpace(entity.getFields(), engine));
        spaces.put(prefix + table + HISTORY_POSTFIX, create(entity.getFields(), engine, HISTORY_POSTFIX));
        handler.handle(Future.succeededFuture(yaml));
    }

    public static Space create(List<EntityField> fields, SpaceEngines engine, String tablePostfix) {
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

    private static List<SpaceIndex> createSpaceIndexes(List<EntityField> fields, String tablePosfix) {
        switch (tablePosfix) {
            case ACTUAL_POSTFIX:
                return Arrays.asList(
                        new SpaceIndex(true, getPrimaryKeyPartsWithSysFrom(fields), SpaceIndexTypes.TREE, ID),
                        new SpaceIndex(false, Collections.singletonList(
                                new SpaceIndexPart(SYS_FROM_FIELD, SpaceAttributeTypes.NUMBER.getName(), false)
                        ), SpaceIndexTypes.TREE, SYS_FROM_FIELD),
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

    private static List<SpaceIndexPart> getPrimaryKeyPartsWithSysFrom(List<EntityField> fields) {
        final List<SpaceIndexPart> spaceIndexParts = EntityFieldUtils.getPrimaryKeyList(fields).stream()
                .map(f -> new SpaceIndexPart(f.getName(),
                        SpaceAttributeTypeUtil.toAttributeType(f.getType()).getName(),
                        f.getNullable()))
                .collect(Collectors.toList());
        spaceIndexParts.add(new SpaceIndexPart(SYS_FROM_FIELD, SpaceAttributeTypes.NUMBER.getName(), false));
        return spaceIndexParts;
    }

    private static List<SpaceIndexPart> getPrimaryKeyParts(List<EntityField> fields) {
        return EntityFieldUtils.getPrimaryKeyList(fields).stream()
                .map(f -> new SpaceIndexPart(f.getName(),
                        SpaceAttributeTypeUtil.toAttributeType(f.getType()).getName(),
                        f.getNullable()))
                .collect(Collectors.toList());
    }

    public static Space createStagingSpace(List<EntityField> fields, SpaceEngines engine) {
        return new Space(
                getStagingAttributes(fields),
                false,
                engine,
                false,
                getShardingKey(fields),
                createSpaceIndexes(fields, STAGING_POSTFIX));
    }

    private static List<String> getShardingKey(List<EntityField> fields) {
        List<String> sk = EntityFieldUtils.getShardingKeyList(fields).stream().map(EntityField::getName).collect(Collectors.toList());
        if (sk.size() == 0) {
            sk = getPrimaryKey(fields);
        }
        return sk;
    }

    private static List<String> getPrimaryKey(List<EntityField> fields) {
        List<String> sk = EntityFieldUtils.getPrimaryKeyList(fields).stream().map(EntityField::getName).collect(Collectors.toList());
        if (sk.size() == 0) {
            sk = Collections.singletonList(ID);
        }
        return sk;
    }

    //The order is synchronized with the AVRO scheme
    private static List<SpaceAttribute> getAttributes(List<EntityField> fields) {
        List<SpaceAttribute> attributes = fields.stream().map(TtCartridgeSchemaGeneratorImpl::toAttribute).collect(Collectors.toList());
        attributes.addAll(
                Arrays.asList(
                        new SpaceAttribute(false, SYS_OP_FIELD, SpaceAttributeTypes.NUMBER),
                        new SpaceAttribute(false, SYS_FROM_FIELD, SpaceAttributeTypes.NUMBER),
                        new SpaceAttribute(true, SYS_TO_FIELD, SpaceAttributeTypes.NUMBER),
                        new SpaceAttribute(false, BUCKET_ID, SpaceAttributeTypes.UNSIGNED))
        );
        return attributes;
    }

    private static List<SpaceAttribute> getStagingAttributes(List<EntityField> fields) {
        List<SpaceAttribute> attributes = fields.stream().map(TtCartridgeSchemaGeneratorImpl::toAttribute).collect(Collectors.toList());
        attributes.addAll(
                Arrays.asList(
                        new SpaceAttribute(false, SYS_OP_FIELD, SpaceAttributeTypes.NUMBER),
                        new SpaceAttribute(false, BUCKET_ID, SpaceAttributeTypes.UNSIGNED))
        );
        return attributes;
    }

    private static SpaceAttribute toAttribute(EntityField field) {
        return new SpaceAttribute(field.getNullable(), field.getName(), SpaceAttributeTypeUtil.toAttributeType(field.getType()));
    }

}
