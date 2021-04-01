package io.arenadata.dtm.query.execution.core.query.exception;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.SourceType;

public class QueriedEntityIsMissingException extends DtmException {

    public QueriedEntityIsMissingException(SourceType sourceType) {
        super(String.format("Queried entity is missing for the specified DATASOURCE_TYPE %s", sourceType));
    }

}
