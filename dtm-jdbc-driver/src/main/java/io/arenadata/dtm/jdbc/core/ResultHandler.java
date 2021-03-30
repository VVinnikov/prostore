package io.arenadata.dtm.jdbc.core;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.time.ZoneId;
import java.util.List;

public interface ResultHandler {

    void handleResultRows(Query query, Field[] fields, List<Tuple> tuples, ZoneId timeZone);

    void handleWarning(SQLWarning sqlWarning);

    void handleError(SQLException sqlException);

    SQLException getException();

    SQLWarning getWarning();
}
