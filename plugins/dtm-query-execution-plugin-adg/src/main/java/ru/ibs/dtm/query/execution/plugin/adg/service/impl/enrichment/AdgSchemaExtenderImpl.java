package ru.ibs.dtm.query.execution.plugin.adg.service.impl.enrichment;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import lombok.val;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.model.metadata.*;
import ru.ibs.dtm.query.execution.plugin.adg.factory.AdgHelperTableNamesFactory;
import ru.ibs.dtm.query.execution.plugin.adg.service.SchemaExtender;

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
    List<DatamartClass> extendedDatamartClasses = new ArrayList<>();
    datamart.getDatamartClassess().forEach(dmClass -> {
      val helperTableNames = helperTableNamesFactory.create(queryRequest.getSystemName(),
              queryRequest.getDatamartMnemonic(),
              dmClass.getLabel());
      dmClass.setMnemonic(dmClass.getMnemonic());
      dmClass.getClassAttributes().addAll(getExtendedColumns());
      extendedDatamartClasses.add(dmClass);
      extendedDatamartClasses.add(getExtendedSchema(dmClass, helperTableNames.getHistory()));
      extendedDatamartClasses.add(getExtendedSchema(dmClass, helperTableNames.getStaging()));
      extendedDatamartClasses.add(getExtendedSchema(dmClass, helperTableNames.getActual()));
    });
    extendedSchema.setDatamartClassess(extendedDatamartClasses);

    return extendedSchema;
  }

  private DatamartClass getExtendedSchema(DatamartClass datamartClass, String tableName) {
    DatamartClass datamartClassExtended = new DatamartClass();
    datamartClassExtended.setLabel(datamartClass.getLabel());
    datamartClassExtended.setMnemonic(tableName);
    datamartClassExtended.setId(UUID.randomUUID());
    List<ClassAttribute> classAttributeList = new ArrayList<>();
    datamartClass.getClassAttributes().forEach(classAttr -> {
      ClassAttribute classAttribute = new ClassAttribute();
      classAttribute.setId(UUID.randomUUID());
      classAttribute.setMnemonic(classAttr.getMnemonic());
      classAttribute.setType(classAttr.getType());
      classAttributeList.add(classAttribute);
    });
    datamartClassExtended.setClassAttributes(classAttributeList);
    return datamartClassExtended;
  }

  private List<ClassAttribute> getExtendedColumns() {
    List<ClassAttribute> classAttributeList = new ArrayList<>();
    classAttributeList.add(generateNewField(SYS_OP_FIELD, ColumnType.INTEGER));
    classAttributeList.add(generateNewField(SYS_TO_FIELD, ColumnType.INTEGER));
    classAttributeList.add(generateNewField(SYS_FROM_FIELD, ColumnType.INTEGER));
    return classAttributeList;
  }

  private ClassAttribute generateNewField(String mnemonic, ColumnType columnType) {
    ClassAttribute classAttribute = new ClassAttribute();
    classAttribute.setId(UUID.randomUUID());
    classAttribute.setMnemonic(mnemonic);
    classAttribute.setType(new TypeMessage(UUID.randomUUID(), columnType));
    return classAttribute;
  }

}
