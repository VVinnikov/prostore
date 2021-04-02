package io.arenadata.dtm.query.execution.plugin.adqm.ddl.factory;

import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryRequest;

public interface AdqmTruncateHistoryQueriesFactory {

    String insertIntoActualQuery(TruncateHistoryRequest request);

    String flushQuery(TruncateHistoryRequest request);

    String optimizeQuery(TruncateHistoryRequest request);
}
