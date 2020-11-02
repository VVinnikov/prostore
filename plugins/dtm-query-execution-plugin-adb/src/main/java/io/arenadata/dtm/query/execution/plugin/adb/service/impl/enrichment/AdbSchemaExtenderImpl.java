package io.arenadata.dtm.query.execution.plugin.adb.service.impl.enrichment;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adb.service.SchemaExtender;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adb.factory.impl.MetadataSqlFactoryImpl.*;


/**
 * Implementing a Logic to Physical Conversion
 */
@Service("adbSchemaExtender")
public class AdbSchemaExtenderImpl implements SchemaExtender {

    private static final List<EntityField> SYSTEM_FIELDS = Arrays.asList(
        generateNewField(SYS_OP_ATTR),
        generateNewField(SYS_TO_ATTR),
        generateNewField(SYS_FROM_ATTR)
    );

    private static EntityField generateNewField(String name) {
        return EntityField.builder()
            .type(ColumnType.INT)
            .name(name)
            .build();
    }

    @Override
    public List<Datamart> generatePhysicalSchemas(List<Datamart> logicalSchemas) {
        return logicalSchemas.stream().map(this::createPhysicalSchema).collect(Collectors.toList());
    }

    private Datamart createPhysicalSchema(Datamart schema) {
        Datamart extendedSchema = new Datamart();
        extendedSchema.setMnemonic(schema.getMnemonic());
        List<Entity> extendedEntities = new ArrayList<>();
        schema.getEntities().forEach(entity -> {
            extendEntityFields(entity.getFields());
            extendedEntities.add(entity);
            extendedEntities.add(getExtendedSchema(entity, "_".concat(HISTORY_TABLE)));
            extendedEntities.add(getExtendedSchema(entity, "_".concat(STAGING_TABLE)));
            extendedEntities.add(getExtendedSchema(entity, "_".concat(ACTUAL_TABLE)));
        });
        extendedSchema.setEntities(extendedEntities);
        return extendedSchema;
    }

    private void extendEntityFields(List<EntityField> entityFields) {
        SYSTEM_FIELDS.stream()
            .filter(sysField -> !entityFields.contains(sysField))
            .forEach(entityFields::add);
    }

    private Entity getExtendedSchema(Entity entity, String tablePostfix) {
        return entity.toBuilder()
            .fields(entity.getFields().stream()
                .map(ef -> ef.toBuilder().build())
                .collect(Collectors.toList()))
            .name(entity.getName() + tablePostfix)
            .build();
    }

}
