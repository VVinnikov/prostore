package ru.ibs.dtm.query.execution.plugin.adg.service.impl.enrichment;

import lombok.val;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.common.model.ddl.EntityField;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.plugin.adg.factory.AdgHelperTableNamesFactory;
import ru.ibs.dtm.query.execution.plugin.adg.service.SchemaExtender;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static ru.ibs.dtm.query.execution.plugin.adg.constants.ColumnFields.*;


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
                .map(ef -> ef.toBuilder().build())
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
