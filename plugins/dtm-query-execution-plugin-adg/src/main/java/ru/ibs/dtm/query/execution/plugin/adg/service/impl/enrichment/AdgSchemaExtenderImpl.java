package ru.ibs.dtm.query.execution.plugin.adg.service.impl.enrichment;

import lombok.val;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.model.metadata.AttributeType;
import ru.ibs.dtm.query.execution.model.metadata.Datamart;
import ru.ibs.dtm.query.execution.model.metadata.DatamartTable;
import ru.ibs.dtm.query.execution.model.metadata.TableAttribute;
import ru.ibs.dtm.query.execution.plugin.adg.factory.AdgHelperTableNamesFactory;
import ru.ibs.dtm.query.execution.plugin.adg.service.SchemaExtender;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static ru.ibs.dtm.query.execution.plugin.adg.constants.ColumnFields.*;


/**
 * Реализация преобразования для логической схемы в физическую
 */
@Service("adgSchemaExtender")
public class AdgSchemaExtenderImpl implements SchemaExtender {
    private final AdgHelperTableNamesFactory helperTableNamesFactory;

    public AdgSchemaExtenderImpl(AdgHelperTableNamesFactory helperTableNamesFactory) {
        this.helperTableNamesFactory = helperTableNamesFactory;
    }

    @Override
    public Datamart generatePhysicalSchema(Datamart datamart, QueryRequest queryRequest) {
        Datamart extendedSchema = new Datamart();
        extendedSchema.setMnemonic(datamart.getMnemonic());
        extendedSchema.setId(UUID.randomUUID());
        List<DatamartTable> extendedDatamartClasses = new ArrayList<>();
        datamart.getDatamartTables().forEach(dmClass -> {
            val helperTableNames = helperTableNamesFactory.create(queryRequest.getSystemName(),
                queryRequest.getDatamartMnemonic(),
                dmClass.getLabel());
            dmClass.setMnemonic(dmClass.getMnemonic());
            dmClass.getTableAttributes().addAll(getExtendedColumns());
            extendedDatamartClasses.add(dmClass);
            extendedDatamartClasses.add(getExtendedSchema(dmClass, helperTableNames.getHistory()));
            extendedDatamartClasses.add(getExtendedSchema(dmClass, helperTableNames.getStaging()));
            extendedDatamartClasses.add(getExtendedSchema(dmClass, helperTableNames.getActual()));
        });
        extendedSchema.setDatamartTables(extendedDatamartClasses);

        return extendedSchema;
    }

    private DatamartTable getExtendedSchema(DatamartTable datamartTable, String tableName) {
        DatamartTable datamartTableExtended = new DatamartTable();
        datamartTableExtended.setLabel(tableName);
        datamartTableExtended.setMnemonic(tableName);
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

    private List<TableAttribute> getExtendedColumns() {
        List<TableAttribute> tableAttributeList = new ArrayList<>();
        tableAttributeList.add(generateNewField(SYS_OP_FIELD, ColumnType.INT));
        tableAttributeList.add(generateNewField(SYS_TO_FIELD, ColumnType.INT));
        tableAttributeList.add(generateNewField(SYS_FROM_FIELD, ColumnType.INT));
        return tableAttributeList;
    }

    private TableAttribute generateNewField(String mnemonic, ColumnType columnType) {
        TableAttribute tableAttribute = new TableAttribute();
        tableAttribute.setId(UUID.randomUUID());
        tableAttribute.setMnemonic(mnemonic);
        tableAttribute.setType(new AttributeType(UUID.randomUUID(), columnType));
        return tableAttribute;
    }

}
