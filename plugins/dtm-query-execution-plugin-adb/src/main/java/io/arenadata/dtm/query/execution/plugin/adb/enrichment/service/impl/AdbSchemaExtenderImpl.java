package io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.impl;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.SchemaExtender;
import lombok.val;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adb.base.factory.Constants.*;

/**
 * Implementing a Logic to Physical Conversion
 */
@Service("adbSchemaExtender")
public class AdbSchemaExtenderImpl implements SchemaExtender {

    @Override
    public Datamart createPhysicalSchema(Datamart schema) {
        Datamart extendedSchema = new Datamart();
        extendedSchema.setMnemonic(schema.getMnemonic());
        List<Entity> extendedEntities = new ArrayList<>();
        schema.getEntities().forEach(entity -> {
            Entity extendedEntity = entity.copy();
            val extendedEntityFields = new ArrayList<>(extendedEntity.getFields());
            extendedEntityFields.addAll(getExtendedColumns());
            extendedEntity.setFields(extendedEntityFields);
            extendedEntities.add(extendedEntity);
            extendedEntities.add(getExtendedSchema(extendedEntity, "_".concat(HISTORY_TABLE)));
            extendedEntities.add(getExtendedSchema(extendedEntity, "_".concat(STAGING_TABLE)));
            extendedEntities.add(getExtendedSchema(extendedEntity, "_".concat(ACTUAL_TABLE)));
        });
        extendedSchema.setEntities(extendedEntities);
        return extendedSchema;
    }

    private Entity getExtendedSchema(Entity entity, String tablePostfix) {
        return entity.toBuilder()
                .fields(entity.getFields().stream()
                        .map(ef -> ef.toBuilder().build())
                        .collect(Collectors.toList()))
                .name(entity.getName() + tablePostfix)
                .build();
    }

    private List<EntityField> getExtendedColumns() {
        List<EntityField> tableAttributeList = new ArrayList<>();
        tableAttributeList.add(generateNewField(SYS_OP_ATTR, false));
        tableAttributeList.add(generateNewField(SYS_TO_ATTR, true));
        tableAttributeList.add(generateNewField(SYS_FROM_ATTR, false));
        return tableAttributeList;
    }

    private EntityField generateNewField(String name, boolean isNullable) {
        return EntityField.builder()
                .type(ColumnType.BIGINT)
                .name(name)
                .nullable(isNullable)
                .build();
    }



}
