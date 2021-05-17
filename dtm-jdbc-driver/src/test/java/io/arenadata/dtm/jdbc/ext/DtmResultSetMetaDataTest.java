package io.arenadata.dtm.jdbc.ext;

import io.arenadata.dtm.jdbc.core.BaseConnection;
import io.arenadata.dtm.jdbc.core.Field;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class DtmResultSetMetaDataTest {

    private final BaseConnection connection = mock(DtmConnectionImpl.class);
    private ResultSetMetaData resultSetMetaData;

    @BeforeEach
    void setUp() {
        Field[] fields = new Field[0];
        resultSetMetaData = new DtmResultSetMetaData(connection, fields);
    }

    @Test
    void unwrap() throws SQLException {
        assertEquals(resultSetMetaData, resultSetMetaData.unwrap(DtmResultSetMetaData.class));
        assertThrows(SQLException.class, () -> resultSetMetaData.unwrap(DtmResultSetTest.class));
    }

    @Test
    void isWrapperFor() throws SQLException {
        assertTrue(resultSetMetaData.isWrapperFor(DtmResultSetMetaData.class));
        assertFalse(resultSetMetaData.isWrapperFor(null));
    }
}