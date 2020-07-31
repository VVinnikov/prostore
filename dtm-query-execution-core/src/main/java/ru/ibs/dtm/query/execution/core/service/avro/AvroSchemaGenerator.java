package ru.ibs.dtm.query.execution.core.service.avro;

import org.apache.avro.Schema;
import ru.ibs.dtm.common.model.ddl.ClassTable;

public interface AvroSchemaGenerator {

    default Schema generateTableSchema(ClassTable table) {
        return generateTableSchema(table, true);
    }

    Schema generateTableSchema(ClassTable table, boolean withSysOpField);
}
