package io.arenadata.dtm.jdbc.ext;

import io.arenadata.dtm.jdbc.core.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DtmConnectionImplTest {

    private BaseConnection connection;
    private final QueryExecutor queryExecutor = mock(QueryExecutorImpl.class);
    private final ConnectionFactory connectionFactory = mock(ConnectionFactoryImpl.class);

    @BeforeEach
    void setUp() throws SQLException {
        String host = "localhost";
        String user = "dtm";
        String schema = "test";
        String url = String.format("jdbc:adtm://%s/", host);
        when(connectionFactory.openConnectionImpl(any(), any(), any(), any(), any()))
                .thenReturn(queryExecutor);
        connection = new DtmConnectionImpl(host, user, schema, null, url);
    }

    @Test
    void unwrap() throws SQLException {
        assertEquals(connection, connection.unwrap(DtmConnectionImpl.class));
        assertThrows(SQLException.class, () -> connection.unwrap(DtmConnectionImplTest.class));
    }

    @Test
    void isWrapperFor() throws SQLException {
        assertTrue(connection.isWrapperFor(DtmConnectionImpl.class));
        assertFalse(connection.isWrapperFor(null));
    }
}