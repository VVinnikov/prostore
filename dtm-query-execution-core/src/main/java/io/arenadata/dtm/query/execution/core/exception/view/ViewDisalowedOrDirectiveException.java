package io.arenadata.dtm.query.execution.core.exception.view;

import io.arenadata.dtm.query.execution.core.exception.DtmException;

public class ViewDisalowedOrDirectiveException extends DtmException {

    public ViewDisalowedOrDirectiveException(String query) {
        super(String.format("Disallowed view or directive in a subquery %s", query));
    }
}
