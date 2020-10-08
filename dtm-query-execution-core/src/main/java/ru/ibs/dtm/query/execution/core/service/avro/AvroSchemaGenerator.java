package ru.ibs.dtm.query.execution.core.service.avro;

import org.apache.avro.Schema;
import ru.ibs.dtm.common.model.ddl.Entity;

public interface AvroSchemaGenerator {

    default Schema generateTableSchema(Entity table) {
        return generateTableSchema(table, true);
    }

    Schema generateTableSchema(Entity table, boolean withSysOpField);
}
