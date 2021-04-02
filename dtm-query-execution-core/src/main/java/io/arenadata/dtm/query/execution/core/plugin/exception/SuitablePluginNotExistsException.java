package io.arenadata.dtm.query.execution.core.plugin.exception;

import io.arenadata.dtm.common.exception.DtmException;

public class SuitablePluginNotExistsException extends DtmException {

    private static final String MESSAGE = "Suitable plugin for the query does not exist.";

    public SuitablePluginNotExistsException() {
        super(MESSAGE);
    }
}
