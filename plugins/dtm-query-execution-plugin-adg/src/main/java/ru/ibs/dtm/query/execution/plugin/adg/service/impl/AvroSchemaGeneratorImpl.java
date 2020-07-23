package ru.ibs.dtm.query.execution.plugin.adg.service.impl;

import org.apache.avro.Schema;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.model.ddl.ClassTypes;
import ru.ibs.dtm.query.execution.plugin.adg.service.AvroSchemaGenerator;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static ru.ibs.dtm.query.execution.plugin.adg.constants.ColumnFields.*;

@Service
public class AvroSchemaGeneratorImpl implements AvroSchemaGenerator {

  @Override
  public Schema generate(ClassTable classTable) {
    List<Schema.Field> fields = classTable.getFields().stream()
        .sorted(Comparator.comparing(ClassField::getOrdinalPosition))
        .map(AvroSchemaGeneratorImpl::toSchemaField)
        .collect(Collectors.toList());
    fields.addAll(addSystemFields());
    Schema recordSchema = Schema.createRecord(classTable.getNameWithSchema(), null, null, false, fields);
    return Schema.createArray(recordSchema);
  }

  private static Schema.Field toSchemaField(ClassField classField) {
    return new Schema.Field(classField.getName(), Schema.create(toSchemaType(classField.getType())));
  }
  //!Важен порядок для схемы! Он должен быть согласован форматом данных отправляемым в топик upload
  private static List<Schema.Field> addSystemFields() {
    return Arrays.asList(
      new Schema.Field(SYS_OP_FIELD, Schema.create(Schema.Type.INT)),
      new Schema.Field(SYS_FROM_FIELD, Schema.create(Schema.Type.INT)),
      new Schema.Field(SYS_TO_FIELD, Schema.create(Schema.Type.INT))
    );
  }

  /**
   * Конвертация в соответстии с требованиями:
   * https://conf.ibs.ru/pages/viewpage.action?pageId=113453625
   */
  private static Schema.Type toSchemaType(ClassTypes classType) {
    {
      switch (classType) {
        case DATE:
        case TIMESTAMP:
        case CHAR:
        case VARCHAR:
        case ANY:
          return Schema.Type.STRING;
        case FLOAT:
          return Schema.Type.FLOAT;
        case DOUBLE:
          return Schema.Type.DOUBLE;
        case INT:
          return Schema.Type.INT;
        case BIGINT:
          return Schema.Type.LONG;
        case BOOLEAN:
          return Schema.Type.BOOLEAN;
        default:
          throw new UnsupportedOperationException(String.format("Не поддержан тип: %s", classType));
      }
    }

  }
}
