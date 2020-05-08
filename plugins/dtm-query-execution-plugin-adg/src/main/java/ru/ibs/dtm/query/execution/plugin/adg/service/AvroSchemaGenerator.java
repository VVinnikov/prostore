package ru.ibs.dtm.query.execution.plugin.adg.service;

import org.apache.avro.Schema;
import ru.ibs.dtm.common.model.ddl.ClassTable;

/**
 * Генератор AVRO схемы
 */
public interface AvroSchemaGenerator {
  Schema generate(ClassTable classTable);
}
