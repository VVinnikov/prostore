package io.arenadata.dtm.query.execution.core.init.service;

import io.arenadata.dtm.common.reader.SourceType;
import io.vertx.core.Future;

public interface CoreInitializationService {

    Future<Void> execute();

    Future<Void> execute(SourceType sourceType);
}