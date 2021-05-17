package io.arenadata.dtm.jdbc.ext;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.jdbc.core.BaseConnection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class DtmParameterMetadataTest {

    private final BaseConnection connection = mock(DtmConnectionImpl.class);
    private ParameterMetaData parameterMetaData;

    @BeforeEach
    void setUp() {
        ColumnType[] paramTypes = new ColumnType[0];
        parameterMetaData = new DtmParameterMetadata(connection, paramTypes);
    }

    @Test
    void unwrap() throws SQLException {
        assertEquals(parameterMetaData, parameterMetaData.unwrap(DtmParameterMetadata.class));
        assertThrows(SQLException.class, () -> parameterMetaData.unwrap(DtmParameterMetadataTest.class));
    }

    @Test
    void isWrapperFor() throws SQLException {
        assertTrue(parameterMetaData.isWrapperFor(DtmParameterMetadata.class));
        assertFalse(parameterMetaData.isWrapperFor(null));
    }
}