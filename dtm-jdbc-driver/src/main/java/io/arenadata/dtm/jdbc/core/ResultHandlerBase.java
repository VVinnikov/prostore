package io.arenadata.dtm.jdbc.core;

import java.sql.SQLException;
import java.sql.SQLWarning;

public abstract class ResultHandlerBase implements ResultHandler {

    private SQLException firstException;
    private SQLException lastException;
    private SQLWarning firstWarning;
    private SQLWarning lastWarning;

    @Override
    public void handleWarning(SQLWarning warning) {
        if (this.firstWarning == null) {
            this.firstWarning = this.lastWarning = warning;
        } else {
            SQLWarning lastWarningResult = this.lastWarning;
            lastWarningResult.setNextException(warning);
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
