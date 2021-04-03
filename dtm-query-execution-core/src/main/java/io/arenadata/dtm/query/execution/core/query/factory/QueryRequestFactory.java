package io.arenadata.dtm.query.execution.core.query.factory;

import io.arenadata.dtm.common.reader.InputQueryRequest;
import io.arenadata.dtm.common.reader.QueryRequest;

public interface QueryRequestFactory {

    QueryRequest create(InputQueryRequest inputQueryRequest);
}
