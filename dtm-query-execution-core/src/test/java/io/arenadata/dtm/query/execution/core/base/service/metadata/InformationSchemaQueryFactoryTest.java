package io.arenadata.dtm.query.execution.core.base.service.metadata;

import io.arenadata.dtm.query.execution.core.base.service.metadata.impl.DataTypeMapper;
import io.arenadata.dtm.query.execution.core.base.service.metadata.impl.InformationSchemaQueryFactory;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class InformationSchemaQueryFactoryTest {

    private DataTypeMapper dataTypeMapper;
    private InformationSchemaQueryFactory informationSchemaQueryFactory;

    @Before
    public void setUp() {
        dataTypeMapper = new DataTypeMapper();
        informationSchemaQueryFactory = new InformationSchemaQueryFactory(dataTypeMapper);
    }

    @Test
    public void testDataTypeMapping() {
        String mappedTypes = informationSchemaQueryFactory.createInitEntitiesQuery().toUpperCase();

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
