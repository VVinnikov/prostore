package io.arenadata.dtm.jdbc.ext;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.jdbc.core.BaseConnection;
import io.arenadata.dtm.jdbc.core.Field;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class DtmResultSetMetaDataTest {

    private final BaseConnection connection = mock(DtmConnectionImpl.class);
    private ResultSetMetaData resultSetMetaData;
    private Field[] fields;

    @BeforeEach
    void setUp() {
        fields = new Field[] {new Field("int_col", ColumnType.INT),
                new Field("int32_col", ColumnType.INT32),
                new Field("bugint_col", ColumnType.BIGINT),
                new Field("boolean_col", ColumnType.BOOLEAN),
                new Field("time_col", ColumnType.TIME),
                new Field("timestamp_col", ColumnType.TIMESTAMP),
                new Field("date_col", ColumnType.DATE),
                new Field("float_col", ColumnType.FLOAT),
                new Field("double_col", ColumnType.DOUBLE),
                new Field("char_col", ColumnType.CHAR),
                new Field("varchar_col", ColumnType.VARCHAR),
                new Field("uuid_col", ColumnType.UUID),
                new Field("link_col", ColumnType.LINK),
                new Field("blob_col", ColumnType.BLOB),
                new Field("any_col", ColumnType.ANY)};
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

    @Test
    void getColumnType() throws SQLException {
        int[] expectedTypes = new int[]{
                JDBCType.INTEGER.getVendorTypeNumber(),
                JDBCType.INTEGER.getVendorTypeNumber(),
                JDBCType.BIGINT.getVendorTypeNumber(),
                JDBCType.BOOLEAN.getVendorTypeNumber(),
                JDBCType.TIME.getVendorTypeNumber(),
                JDBCType.TIMESTAMP.getVendorTypeNumber(),
                JDBCType.DATE.getVendorTypeNumber(),
                JDBCType.FLOAT.getVendorTypeNumber(),
                JDBCType.DOUBLE.getVendorTypeNumber(),
                JDBCType.CHAR.getVendorTypeNumber(),
                JDBCType.VARCHAR.getVendorTypeNumber(),
                JDBCType.VARCHAR.getVendorTypeNumber(),
                JDBCType.VARCHAR.getVendorTypeNumber(),
                JDBCType.BLOB.getVendorTypeNumber(),
                JDBCType.VARCHAR.getVendorTypeNumber()
        };
        int[] jdbcTypes = new int[fields.length];
        for (int i = 0; i < fields.length; i++) {
            jdbcTypes[i] = resultSetMetaData.getColumnType(i + 1);
        }
        assertArrayEquals(expectedTypes, jdbcTypes);
    }
}