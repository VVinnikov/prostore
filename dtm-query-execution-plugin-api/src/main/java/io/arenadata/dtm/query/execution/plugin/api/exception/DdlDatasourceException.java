package io.arenadata.dtm.query.execution.plugin.api.exception;

public class DdlDatasourceException extends DataSourceException {

    public DdlDatasourceException(String s) {
        super(s);
    }

    public DdlDatasourceException(String s, Throwable throwable) {
        super(s, throwable);
    }
}