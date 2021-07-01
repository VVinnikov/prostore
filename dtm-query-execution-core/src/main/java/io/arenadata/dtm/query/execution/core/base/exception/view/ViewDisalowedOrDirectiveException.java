package io.arenadata.dtm.query.execution.core.base.exception.view;

import io.arenadata.dtm.common.exception.DtmException;

public class ViewDisalowedOrDirectiveException extends DtmException {

    public ViewDisalowedOrDirectiveException(String query) {
        super(String.format("Disallowed view or directive in a subquery [%s]", query));
    }

    public ViewDisalowedOrDirectiveException(String query, String cause) {
        super(String.format("Disallowed view or directive in a subquery [%s] " +
                "%nCause: %s", query, cause));
    }
}
