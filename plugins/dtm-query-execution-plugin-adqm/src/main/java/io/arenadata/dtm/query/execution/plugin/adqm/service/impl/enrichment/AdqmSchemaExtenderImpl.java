package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.enrichment;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adqm.factory.AdqmHelperTableNamesFactory;
import io.arenadata.dtm.query.execution.plugin.adqm.service.SchemaExtender;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adqm.utils.Constants.*;


/**
 * Implementing a Logic to Physical Conversion
 */
@Service("adqmSchemaExtender")
public class AdqmSchemaExtenderImpl implements SchemaExtender {
    private final AdqmHelperTableNamesFactory helperTableNamesFactory;

    @Autowired
    public AdqmSchemaExtenderImpl(AdqmHelperTableNamesFactory helperTableNamesFactory) {
        this.helperTableNamesFactory = helperTableNamesFactory;
    }

    public static List<EntityField> getExtendedColumns() {
        return Arrays.asList(
            generateNewField(SYS_OP_FIELD, ColumnType.INT),
            generateNewField(SYS_TO_FIELD, ColumnType.BIGINT),
            generateNewField(SYS_FROM_FIELD, ColumnType.BIGINT),
            generateNewField(SIGN_FIELD, ColumnType.INT),
            generateNewField(SYS_CLOSE_DATE_FIELD, ColumnType.DATE)
        );
    }

    private static EntityField generateNewField(String name, ColumnType columnType) {
        return EntityField.builder()
            .type(columnType)
            .name(name)
            .build();
    }

    @Override
    public Datamart createPhysicalSchema(Datamart logicalSchema, String systemName) {
        Datamart extendedSchema = new Datamart();
        extendedSchema.setMnemonic(logicalSchema.getMnemonic());
        List<Entity> extendedEntities = new ArrayList<>();
        logicalSchema.getEntities().forEach(entity -> {
            val helperTableNames = helperTableNamesFactory.create(systemName,
                logicalSchema.getMnemonic(),
                entity.getName());
            entity.setSchema(helperTableNames.getSchema());
            val extendedEntityFields = new ArrayList<>(entity.getFields());
            extendedEntityFields.addAll(getExtendedColumns());
            entity.setFields(extendedEntityFields);
            extendedEntities.add(entity);
            extendedEntities.add(getExtendedSchema(entity, helperTableNames.getActual()));
            extendedEntities.add(getExtendedSchema(entity, helperTableNames.getActualShard()));
        });
        extendedEntities.stream()
            .findFirst()
            .ifPresent(datamartTable -> extendedSchema.setMnemonic(datamartTable.getSchema()));
        extendedSchema.setEntities(extendedEntities);
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

}
