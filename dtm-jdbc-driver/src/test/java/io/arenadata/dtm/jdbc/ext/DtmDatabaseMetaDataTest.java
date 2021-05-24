package io.arenadata.dtm.jdbc.ext;

import io.arenadata.dtm.jdbc.core.BaseConnection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class DtmDatabaseMetaDataTest {

    private final BaseConnection connection = mock(DtmConnectionImpl.class);
    private DatabaseMetaData databaseMetaData;

    @BeforeEach
    void setUp() {
        databaseMetaData = new DtmDatabaseMetaData(connection);
    }

    @Test
    void unwrap() throws SQLException {
        assertEquals(databaseMetaData, databaseMetaData.unwrap(DtmDatabaseMetaData.class));
        assertThrows(SQLException.class, () -> databaseMetaData.unwrap(DtmDatabaseMetaDataTest.class));
    }

    @Test
    void isWrapperFor() throws SQLException {
        assertTrue(databaseMetaData.isWrapperFor(DtmDatabaseMetaData.class));
        assertFalse(databaseMetaData.isWrapperFor(null));
    }
}