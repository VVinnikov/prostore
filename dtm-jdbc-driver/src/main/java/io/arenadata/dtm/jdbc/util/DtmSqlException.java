package io.arenadata.dtm.jdbc.util;

import java.sql.SQLException;

public class DtmSqlException extends SQLException {
    public DtmSqlException(String message) {
        super(message);
    }
}
