package io.arenadata.dtm.query.execution.plugin.adg.factory;

import io.arenadata.dtm.query.execution.plugin.api.dto.TruncateHistoryParams;
import io.vertx.core.Future;

public interface AdgTruncateHistoryConditionFactory {

    Future<String> create(TruncateHistoryParams params);
}
