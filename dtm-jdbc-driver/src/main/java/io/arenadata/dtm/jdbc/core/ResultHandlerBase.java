package io.arenadata.dtm.jdbc.core;

import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.time.ZoneId;
import java.util.List;

public class ResultHandlerBase implements ResultHandler {

    private SQLException firstException;
    private SQLException lastException;
    private SQLWarning firstWarning;
    private SQLWarning lastWarning;

    public ResultHandlerBase() {
    }

    @Override
    public void handleResultRows(Query query, List<Field[]> fields, List<ColumnMetadata> metadata, ZoneId timeZone) {

    }

    @Override
    public void handleWarning(SQLWarning warning) {
        if (this.firstWarning == null) {
            this.firstWarning = this.lastWarning = warning;
        } else {
            SQLWarning lastWarning = this.lastWarning;
            lastWarning.setNextException(warning);
            this.lastWarning = warning;
        }
    }

    @Override
    public void handleError(SQLException sqlException) {
        if (this.firstException == null) {
            this.firstException = this.lastException = sqlException;
        } else {
            this.lastException.setNextException(sqlException);
            this.lastException = sqlException;
        }
    }

    @Override
    public SQLException getException() {
        return this.firstException;
    }

    @Override
    public SQLWarning getWarning() {
        return this.firstWarning;
    }
}
