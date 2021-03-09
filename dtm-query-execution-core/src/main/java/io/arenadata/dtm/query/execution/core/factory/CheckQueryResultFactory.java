package io.arenadata.dtm.query.execution.core.factory;

import io.arenadata.dtm.common.reader.QueryResult;

public interface CheckQueryResultFactory {

    QueryResult create(String result);
}
