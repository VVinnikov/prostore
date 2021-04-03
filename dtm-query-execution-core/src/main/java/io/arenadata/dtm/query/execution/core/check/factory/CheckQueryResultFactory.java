package io.arenadata.dtm.query.execution.core.check.factory;

import io.arenadata.dtm.common.reader.QueryResult;

public interface CheckQueryResultFactory {

    QueryResult create(String result);
}
