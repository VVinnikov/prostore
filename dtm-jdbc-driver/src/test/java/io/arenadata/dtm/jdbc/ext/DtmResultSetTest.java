package io.arenadata.dtm.jdbc.ext;

import io.arenadata.dtm.jdbc.core.BaseConnection;
import io.arenadata.dtm.jdbc.core.BaseStatement;
import io.arenadata.dtm.jdbc.core.Field;
import io.arenadata.dtm.jdbc.core.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class DtmResultSetTest {

    private final BaseConnection connection = mock(DtmConnectionImpl.class);
    private final BaseStatement statement = mock(DtmStatement.class);
    private ResultSet resultSet;

    @BeforeEach
    void setUp() {
        Field[] fields = new Field[0];
        List<Tuple> tuples = new ArrayList<>();
        ZoneId zoneId = ZoneId.of("UTC");
        resultSet = new DtmResultSet(connection, statement, fields, tuples, zoneId);
    }

    @Test
    void unwrap() throws SQLException {
        assertEquals(resultSet, resultSet.unwrap(DtmResultSet.class));
        assertThrows(SQLException.class, () -> resultSet.unwrap(DtmResultSetTest.class));
    }

    @Test
    void isWrapperFor() throws SQLException {
        assertTrue(resultSet.isWrapperFor(DtmResultSet.class));
        assertFalse(resultSet.isWrapperFor(null));
    }
}