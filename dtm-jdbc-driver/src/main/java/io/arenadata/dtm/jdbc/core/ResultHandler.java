package io.arenadata.dtm.jdbc.core;

import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.time.ZoneId;
import java.util.List;

public interface ResultHandler {

    void handleResultRows(Query query, List<Field[]> fields, List<ColumnMetadata> metadata, ZoneId timeZone);

    void handleWarning(SQLWarning sqlWarning);

    void handleError(SQLException sqlException);

    SQLException getException();

    SQLWarning getWarning();
}
