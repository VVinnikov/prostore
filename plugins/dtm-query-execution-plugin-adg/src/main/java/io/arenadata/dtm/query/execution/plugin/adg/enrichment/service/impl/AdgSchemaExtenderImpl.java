package io.arenadata.dtm.query.execution.plugin.adg.enrichment.service.impl;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adg.base.factory.AdgHelperTableNamesFactory;
import io.arenadata.dtm.query.execution.plugin.adg.enrichment.service.SchemaExtender;
import lombok.val;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adg.base.utils.ColumnFields.*;


/**
 * Implementing a Logic to Physical Conversion
 */
@Service("adgSchemaExtender")
public class AdgSchemaExtenderImpl implements SchemaExtender {
    private final AdgHelperTableNamesFactory helperTableNamesFactory;

    public AdgSchemaExtenderImpl(AdgHelperTableNamesFactory helperTableNamesFactory) {
        this.helperTableNamesFactory = helperTableNamesFactory;
    }

    @Override
    public Datamart createPhysicalSchema(Datamart logicalSchema, String systemName) {
        Datamart extendedSchema = new Datamart();
        extendedSchema.setMnemonic(logicalSchema.getMnemonic());
        List<Entity> extendedDatamartClasses = new ArrayList<>();
        logicalSchema.getEntities().forEach(entity -> {
            val helperTableNames = helperTableNamesFactory.create(systemName,
                    logicalSchema.getMnemonic(),
                    entity.getName());
            val extendedEntityFields = new ArrayList<>(entity.getFields());
            extendedEntityFields.addAll(getExtendedColumns());
            entity.setFields(extendedEntityFields);
            extendedDatamartClasses.add(entity);
            extendedDatamartClasses.add(getExtendedSchema(entity, helperTableNames.getHistory()));
            extendedDatamartClasses.add(getExtendedSchema(entity, helperTableNames.getStaging()));
            extendedDatamartClasses.add(getExtendedSchema(entity, helperTableNames.getActual()));
        });
        extendedSchema.setEntities(extendedDatamartClasses);
        return extendedSchema;
    }


    private Entity getExtendedSchema(Entity entity, String tableName) {
        return entity.toBuilder()
                .fields(entity.getFields().stream()
                        .map(EntityField::copy)
                        .collect(Collectors.toList()))
                .name(tableName)
                .build();
    }

    private List<EntityField> getExtendedColumns() {
        List<EntityField> tableAttributeList = new ArrayList<>();
        tableAttributeList.add(generateNewField(SYS_OP_FIELD));
        tableAttributeList.add(generateNewField(SYS_TO_FIELD));
        tableAttributeList.add(generateNewField(SYS_FROM_FIELD));
        return tableAttributeList;
    }

    private EntityField generateNewField(String name) {
        return EntityField.builder()
                .type(ColumnType.INT)
                .name(name)
                .build();
    }

}
