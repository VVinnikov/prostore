package io.arenadata.dtm.query.execution.core.service.avro;

import io.arenadata.dtm.common.model.ddl.Entity;
import org.apache.avro.Schema;

public interface AvroSchemaGenerator {

    default Schema generateTableSchema(Entity table) {
        return generateTableSchema(table, true);
    }

    Schema generateTableSchema(Entity table, boolean withSysOpField);
}
