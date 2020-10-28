package io.arenadata.dtm.query.execution.core.service.cache;

import io.vertx.core.Future;

public interface CacheLoader {

    Future<Void> load();
}
