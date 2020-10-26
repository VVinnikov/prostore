package io.arenadata.dtm.jdbc.util;

import java.sql.SQLException;

public class DtmException extends SQLException {
    public DtmException(String message) {
        super(message);
    }
}
