package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.enrichment;

import lombok.val;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.model.metadata.AttributeType;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.model.metadata.DatamartTable;
import ru.ibs.dtm.query.execution.model.metadata.TableAttribute;
import ru.ibs.dtm.query.execution.plugin.adqm.factory.AdqmHelperTableNamesFactory;
import ru.ibs.dtm.query.execution.plugin.adqm.service.SchemaExtender;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static ru.ibs.dtm.query.execution.plugin.adqm.common.Constants.*;


/**
 * Реализация преобразования для логической схемы в физическую
 */
@Service("adqmSchemaExtender")
public class AdqmSchemaExtenderImpl implements SchemaExtender {
    private final AdqmHelperTableNamesFactory helperTableNamesFactory;

    public AdqmSchemaExtenderImpl(AdqmHelperTableNamesFactory helperTableNamesFactory) {
        this.helperTableNamesFactory = helperTableNamesFactory;
    }

    public static List<TableAttribute> getExtendedColumns() {
        return Arrays.asList(
            generateNewField(SYS_OP_FIELD, ColumnType.INT),
            generateNewField(SYS_TO_FIELD, ColumnType.INT),
            generateNewField(SYS_FROM_FIELD, ColumnType.INT),
            generateNewField(SIGN_FIELD, ColumnType.INT),
            generateNewField(CLOSE_DATE_FIELD, ColumnType.DATE)
        );
    }

    private static TableAttribute generateNewField(String mnemonic, ColumnType columnType) {
        TableAttribute tableAttribute = new TableAttribute();
        tableAttribute.setId(UUID.randomUUID());
        tableAttribute.setMnemonic(mnemonic);
        tableAttribute.setType(new AttributeType(UUID.randomUUID(), columnType));
        return tableAttribute;
    }

    @Override
    public List<Datamart> generatePhysicalSchema(List<Datamart> logicalSchemas, QueryRequest request) {
        return logicalSchemas.stream().map(ls -> createPhysicalSchema(ls, request.getSystemName()))
            .collect(Collectors.toList());
    }

    private Datamart createPhysicalSchema(Datamart logicalSchema, String systemName) {
        Datamart extendedSchema = new Datamart();
        extendedSchema.setMnemonic(logicalSchema.getMnemonic());
        extendedSchema.setId(UUID.randomUUID());
        List<DatamartTable> extendedDatamartClasses = new ArrayList<>();
        logicalSchema.getDatamartTables().forEach(dmClass -> {
            val helperTableNames = helperTableNamesFactory.create(systemName,
                logicalSchema.getMnemonic(),
                dmClass.getLabel());
            dmClass.setDatamartMnemonic(helperTableNames.getSchema());
            dmClass.setMnemonic(dmClass.getMnemonic());
            dmClass.getTableAttributes().addAll(getExtendedColumns());
            extendedDatamartClasses.add(dmClass);
            extendedDatamartClasses.add(getExtendedSchema(dmClass, helperTableNames.getActual()));
            extendedDatamartClasses.add(getExtendedSchema(dmClass, helperTableNames.getActualShard()));
        });
        extendedDatamartClasses.stream()
            .findFirst()
            .ifPresent(datamartTable -> extendedSchema.setMnemonic(datamartTable.getDatamartMnemonic()));
        extendedSchema.setDatamartTables(extendedDatamartClasses);
        return extendedSchema;
    }

    private DatamartTable getExtendedSchema(DatamartTable datamartTable, String tableName) {
        DatamartTable datamartTableExtended = new DatamartTable();
        datamartTableExtended.setLabel(tableName);
        datamartTableExtended.setMnemonic(tableName);
        datamartTableExtended.setDatamartMnemonic(datamartTable.getDatamartMnemonic());
        datamartTableExtended.setId(UUID.randomUUID());
        List<TableAttribute> tableAttributeList = new ArrayList<>();
        datamartTable.getTableAttributes().forEach(classAttr -> {
            TableAttribute tableAttribute = new TableAttribute();
            tableAttribute.setId(UUID.randomUUID());
            tableAttribute.setMnemonic(classAttr.getMnemonic());
            tableAttribute.setType(classAttr.getType());
            tableAttributeList.add(tableAttribute);
        });
        datamartTableExtended.setTableAttributes(tableAttributeList);
        return datamartTableExtended;
    }

}
