package io.arenadata.dtm.query.execution.core.base.service.metadata.impl;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import lombok.Getter;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Component
@Getter
public class DataTypeMapper {

    private final Map<String, String> hsqlToLogicalSchemaMapping;

    public DataTypeMapper() {
        Map<String, String> typesMapping = new HashMap<>();

        typesMapping.put("CHARACTER VARYING", ColumnType.VARCHAR.name());
        typesMapping.put("CHARACTER", ColumnType.CHAR.name());
        typesMapping.put("LONGVARCHAR", ColumnType.VARCHAR.name());
        typesMapping.put("INTEGER", ColumnType.INT.name());
        typesMapping.put("SMALLINT", ColumnType.INT32.name());
        typesMapping.put("TINYINT", ColumnType.INT32.name());
        typesMapping.put("DOUBLE PRECISION", ColumnType.DOUBLE.name());
        typesMapping.put("DECIMAL", ColumnType.DOUBLE.name());
        typesMapping.put("DEC", ColumnType.DOUBLE.name());
        typesMapping.put("NUMERIC", ColumnType.DOUBLE.name());

        hsqlToLogicalSchemaMapping = Collections.unmodifiableMap(typesMapping);
    }

}