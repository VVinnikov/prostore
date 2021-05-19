package io.arenadata.dtm.query.execution.core.base.service.metadata.impl;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class DataTypeMapper {

    private final Map<String, String> typesMapping;

    public DataTypeMapper() {
        this.typesMapping = new HashMap<>();

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
    }

    public String selectDataType() {
        StringBuilder result = new StringBuilder();

        result.append(" case ");
        typesMapping.entrySet().forEach(entry -> {
            result.append(String.format(" when DATA_TYPE = '%s' then '%s' ", entry.getKey(), entry.getValue()));
        });
        result.append(" else DATA_TYPE end as DATA_TYPE");

        return result.toString();
    }
}
