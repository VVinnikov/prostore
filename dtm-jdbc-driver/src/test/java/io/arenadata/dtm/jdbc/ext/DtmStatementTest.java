package io.arenadata.dtm.jdbc.ext;

import io.arenadata.dtm.jdbc.core.BaseConnection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class DtmStatementTest {

    private final BaseConnection connection = mock(DtmConnectionImpl.class);
    private Statement statement;

    @BeforeEach
    void setUp() {
        int rsType = 0;
        int rsConcurrency = 0;
        statement = new DtmStatement(connection, rsType, rsConcurrency);
    }

    @Test
    void unwrap() throws SQLException {
        assertEquals(statement, statement.unwrap(DtmStatement.class));
        assertThrows(SQLException.class, () -> statement.unwrap(DtmStatementTest.class));
    }

    @Test
    void isWrapperFor() throws SQLException {
        assertTrue(statement.isWrapperFor(DtmStatement.class));
        assertFalse(statement.isWrapperFor(null));
    }
}