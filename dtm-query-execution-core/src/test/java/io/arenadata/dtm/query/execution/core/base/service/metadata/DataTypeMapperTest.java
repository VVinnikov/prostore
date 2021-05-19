package io.arenadata.dtm.query.execution.core.base.service.metadata;

import io.arenadata.dtm.query.execution.core.base.service.metadata.impl.DataTypeMapper;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class DataTypeMapperTest {

    private DataTypeMapper dataTypeMapper = new DataTypeMapper();

    @Test
    public void testDataTypeMapping() {
        String mappedTypes = dataTypeMapper.selectDataType().toUpperCase();

        assertTrue(mappedTypes.contains("WHEN DATA_TYPE = 'CHARACTER' THEN 'CHAR'"));
        assertTrue(mappedTypes.contains("WHEN DATA_TYPE = 'CHARACTER VARYING' THEN 'VARCHAR'"));
        assertTrue(mappedTypes.contains("WHEN DATA_TYPE = 'LONGVARCHAR' THEN 'VARCHAR'"));
        assertTrue(mappedTypes.contains("WHEN DATA_TYPE = 'SMALLINT' THEN 'INT32'"));
        assertTrue(mappedTypes.contains("WHEN DATA_TYPE = 'INTEGER' THEN 'INT'"));
        assertTrue(mappedTypes.contains("WHEN DATA_TYPE = 'TINYINT' THEN 'INT32'"));
        assertTrue(mappedTypes.contains("WHEN DATA_TYPE = 'DOUBLE PRECISION' THEN 'DOUBLE'"));
        assertTrue(mappedTypes.contains("WHEN DATA_TYPE = 'DECIMAL' THEN 'DOUBLE'"));
        assertTrue(mappedTypes.contains("WHEN DATA_TYPE = 'DEC' THEN 'DOUBLE'"));
        assertTrue(mappedTypes.contains("WHEN DATA_TYPE = 'NUMERIC' THEN 'DOUBLE'"));
    }

}
