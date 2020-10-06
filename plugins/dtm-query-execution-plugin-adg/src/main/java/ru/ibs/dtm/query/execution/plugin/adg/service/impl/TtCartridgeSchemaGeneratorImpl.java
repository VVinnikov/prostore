package ru.ibs.dtm.query.execution.plugin.adg.service.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.val;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.model.ddl.EntityField;
import ru.ibs.dtm.common.model.ddl.EntityFieldUtils;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.OperationYaml;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.schema.*;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtCartridgeSchemaGenerator;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import static ru.ibs.dtm.query.execution.plugin.adg.constants.ColumnFields.*;

@Service
public class TtCartridgeSchemaGeneratorImpl implements TtCartridgeSchemaGenerator {

    @Override
    public void generate(DdlRequestContext context, OperationYaml yaml, Handler<AsyncResult<OperationYaml>> handler) {
        if (yaml.getSpaces() == null) {
            yaml.setSpaces(new LinkedHashMap<>());
        }
        val spaces = yaml.getSpaces();
        QueryRequest queryRequest = context.getRequest().getQueryRequest();
        String prefix = queryRequest.getSystemName() + "__" + queryRequest.getDatamartMnemonic() + "__";
        Entity entity = context.getRequest().getEntity();
        int indexComma = entity.getName().indexOf(".");
        String table = entity.getName().substring(indexComma + 1).toLowerCase();

        spaces.put(prefix + table + ACTUAL_POSTFIX, create(entity.getFields()));
        spaces.put(prefix + table + STAGING_POSTFIX, createStagingSpace(entity.getFields()));
        spaces.put(prefix + table + HISTORY_POSTFIX, create(entity.getFields()));
        handler.handle(Future.succeededFuture(yaml));
    }

    public static Space create(List<EntityField> fields) {
        List<SpaceIndexPart> primaryKeyParts = getPrimaryKeyParts(fields);
        primaryKeyParts.add(new SpaceIndexPart(SYS_FROM_FIELD, SpaceAttributeTypes.NUMBER.getName(), false));
        return new Space(
                getAttributes(fields),
                false,
                SpaceEngines.MEMTX,
                false,
                getShardingKey(fields),
                Arrays.asList(
                        new SpaceIndex(true, primaryKeyParts, SpaceIndexTypes.TREE, ID),
                        new SpaceIndex(false, Collections.singletonList(
                                new SpaceIndexPart(BUCKET_ID, SpaceAttributeTypes.UNSIGNED.getName(), false)
                        ), SpaceIndexTypes.TREE, BUCKET_ID)
                ));
    }

    private static List<SpaceIndexPart> getPrimaryKeyParts(List<EntityField> fields) {
        return EntityFieldUtils.getPrimaryKeyList(fields).stream()
                .map(f -> new SpaceIndexPart(f.getName(), SpaceAttributeTypeUtil.toAttributeType(f.getType()).getName(), f.getNullable()))
                .collect(Collectors.toList());
    }

    public static Space createStagingSpace(List<EntityField> fields) {
        return new Space(
                getStagingAttributes(fields),
                false,
                SpaceEngines.MEMTX,
                false,
                getShardingKey(fields),
                Arrays.asList(
                        new SpaceIndex(true, getPrimaryKeyParts(fields), SpaceIndexTypes.TREE, ID),
                        new SpaceIndex(false, Collections.singletonList(
                                new SpaceIndexPart(BUCKET_ID, SpaceAttributeTypes.UNSIGNED.getName(), false)
                        ), SpaceIndexTypes.TREE, BUCKET_ID)
                ));
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
