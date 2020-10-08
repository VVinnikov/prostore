package ru.ibs.dtm.query.execution.core.factory;

import ru.ibs.dtm.common.reader.InputQueryRequest;
import ru.ibs.dtm.common.reader.QueryRequest;

public interface QueryRequestFactory {

    QueryRequest create(InputQueryRequest inputQueryRequest);
}
