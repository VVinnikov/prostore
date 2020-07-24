package ru.ibs.dtm.query.execution.plugin.adg.service.impl;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.model.ddl.ColumnType;
import ru.ibs.dtm.query.execution.plugin.adg.service.AvroSchemaGenerator;

import java.util.Arrays;

class AvroSchemaGeneratorImplTest {

  AvroSchemaGenerator schemaGenerator;

  @BeforeEach
  void setup() {
    schemaGenerator = new AvroSchemaGeneratorImpl();
  }

  @Test
  public void testGenerate() {
    Schema schema = schemaGenerator.generate(new ClassTable("test.test_", Arrays.asList(
      new ClassField(0,"id", ColumnType.INT.name(), false, 1, 1, ""),
      new ClassField(0, "test", ColumnType.VARCHAR.name(), true, 1, 1, "")
    )));
    Assertions.assertNotNull(schema);
  }
}
