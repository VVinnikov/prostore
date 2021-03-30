package io.arenadata.dtm.query.execution.core.exception.query;

import io.arenadata.dtm.common.exception.DtmException;

public class NoSingleDataSourceContainsAllEntitiesException extends DtmException {

    public NoSingleDataSourceContainsAllEntitiesException() {
        super("No single datasource contains all queried entities");
    }

}
