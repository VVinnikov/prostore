package ru.ibs.dtm.query.execution.core.service;

import ru.ibs.dtm.common.reader.QueryRequest;

public interface SemicolonRemover {
    QueryRequest remove(QueryRequest queryRequest);
}
