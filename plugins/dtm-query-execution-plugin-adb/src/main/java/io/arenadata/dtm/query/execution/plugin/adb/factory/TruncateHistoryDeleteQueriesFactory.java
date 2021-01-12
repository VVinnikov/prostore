package io.arenadata.dtm.query.execution.plugin.adb.factory;

import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryParams;

import java.util.List;

public interface TruncateHistoryDeleteQueriesFactory {

    List<String> create(TruncateHistoryParams params);

    String createWithSysCn(TruncateHistoryParams params);
}
