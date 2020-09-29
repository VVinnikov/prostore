package ru.ibs.dtm.query.execution.plugin.adb.service.impl.enrichment;

import lombok.val;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.model.ddl.EntityField;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.plugin.adb.service.SchemaExtender;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static ru.ibs.dtm.query.execution.plugin.adb.factory.impl.MetadataSqlFactoryImpl.*;


/**
 * Implementing a Logic to Physical Conversion
 */
@Service("adbSchemaExtender")
public class AdbSchemaExtenderImpl implements SchemaExtender {

    @Override
    public List<Datamart> generatePhysicalSchemas(List<Datamart> logicalSchemas) {
        return logicalSchemas.stream().map(this::createPhysicalSchema).collect(Collectors.toList());
    }

    private Datamart createPhysicalSchema(Datamart schema) {
        Datamart extendedSchema = new Datamart();
        extendedSchema.setMnemonic(schema.getMnemonic());
        List<Entity> extendedEntities = new ArrayList<>();
        schema.getEntities().forEach(entity -> {
            val extendedEntityFields = new ArrayList<>(entity.getFields());
            extendedEntityFields.addAll(getExtendedColumns());
            entity.setFields(extendedEntityFields);
            extendedEntities.add(entity);
            extendedEntities.add(getExtendedSchema(entity, "_".concat(HISTORY_TABLE)));
            extendedEntities.add(getExtendedSchema(entity, "_".concat(STAGING_TABLE)));
            extendedEntities.add(getExtendedSchema(entity, "_".concat(ACTUAL_TABLE)));
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
        tableAttributeList.add(generateNewField(SYS_OP_ATTR));
        tableAttributeList.add(generateNewField(SYS_TO_ATTR));
        tableAttributeList.add(generateNewField(SYS_FROM_ATTR));
        return tableAttributeList;
    }

    private EntityField generateNewField(String name) {
        return EntityField.builder()
            .type(ColumnType.INT)
            .name(name)
            .build();
    }

}
