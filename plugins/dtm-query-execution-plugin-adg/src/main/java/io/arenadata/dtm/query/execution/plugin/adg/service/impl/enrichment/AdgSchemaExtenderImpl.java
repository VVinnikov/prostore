package io.arenadata.dtm.query.execution.plugin.adg.service.impl.enrichment;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adg.factory.AdgHelperTableNamesFactory;
import io.arenadata.dtm.query.execution.plugin.adg.service.SchemaExtender;
import lombok.val;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adg.constants.ColumnFields.*;


/**
 * Implementing a Logic to Physical Conversion
 */
@Service("adgSchemaExtender")
public class AdgSchemaExtenderImpl implements SchemaExtender {
    private static final List<EntityField> SYSTEM_FIELDS = Arrays.asList(
        generateNewField(SYS_OP_FIELD),
        generateNewField(SYS_TO_FIELD),
        generateNewField(SYS_FROM_FIELD)
    );
    private final AdgHelperTableNamesFactory helperTableNamesFactory;

    public AdgSchemaExtenderImpl(AdgHelperTableNamesFactory helperTableNamesFactory) {
        this.helperTableNamesFactory = helperTableNamesFactory;
    }

    private static EntityField generateNewField(String name) {
        return EntityField.builder()
            .type(ColumnType.INT)
            .name(name)
            .build();
    }

    @Override
    public List<Datamart> generatePhysicalSchema(List<Datamart> logicalSchemas, QueryRequest request) {
        return logicalSchemas.stream().map(ls -> createPhysicalSchema(ls, request.getEnvName()))
            .collect(Collectors.toList());
    }

    private Datamart createPhysicalSchema(Datamart logicalSchema, String systemName) {
        Datamart extendedSchema = new Datamart();
        extendedSchema.setMnemonic(logicalSchema.getMnemonic());
        List<Entity> extendedDatamartClasses = new ArrayList<>();
        logicalSchema.getEntities().forEach(entity -> {
            val helperTableNames = helperTableNamesFactory.create(systemName,
                logicalSchema.getMnemonic(),
                entity.getName());
            extendEntityFields(entity.getFields());
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
                .map(ef -> ef.toBuilder().build())
                .collect(Collectors.toList()))
            .name(tableName)
            .build();
    }

    private void extendEntityFields(List<EntityField> entityFields) {
        SYSTEM_FIELDS.stream()
            .filter(sysField -> !entityFields.contains(sysField))
            .forEach(entityFields::add);
    }

}
