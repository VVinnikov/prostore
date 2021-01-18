package io.arenadata.dtm.query.execution.plugin.adqm.factory;

import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryRequest;

public interface AdqmTruncateHistoryQueriesFactory {

    String insertIntoActualQuery(TruncateHistoryRequest params);

    String flushQuery(TruncateHistoryRequest params);

    String optimizeQuery(TruncateHistoryRequest params);
}
