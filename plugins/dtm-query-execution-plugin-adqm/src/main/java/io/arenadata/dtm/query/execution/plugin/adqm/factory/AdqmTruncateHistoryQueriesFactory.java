package io.arenadata.dtm.query.execution.plugin.adqm.factory;

import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryParams;

public interface AdqmTruncateHistoryQueriesFactory {

    String insertIntoActualQuery(TruncateHistoryParams params);

    String flushQuery(TruncateHistoryParams params);

    String optimizeQuery(TruncateHistoryParams params);
}
