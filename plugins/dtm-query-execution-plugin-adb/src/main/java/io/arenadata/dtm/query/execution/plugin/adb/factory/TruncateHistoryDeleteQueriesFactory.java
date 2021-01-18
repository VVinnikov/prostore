package io.arenadata.dtm.query.execution.plugin.adb.factory;

import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryRequest;

import java.util.List;

public interface TruncateHistoryDeleteQueriesFactory {

    List<String> create(TruncateHistoryRequest params);

    String createWithSysCn(TruncateHistoryRequest params);
}
