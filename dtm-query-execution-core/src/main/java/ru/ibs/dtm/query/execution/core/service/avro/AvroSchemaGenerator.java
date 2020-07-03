package ru.ibs.dtm.query.execution.core.service.avro;

import org.apache.avro.Schema;
import ru.ibs.dtm.common.model.ddl.ClassTable;

public interface AvroSchemaGenerator {

    Schema generateTableSchema(ClassTable table);
}
