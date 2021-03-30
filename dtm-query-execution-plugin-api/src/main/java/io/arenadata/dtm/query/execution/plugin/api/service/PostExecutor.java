package io.arenadata.dtm.query.execution.plugin.api.service;

import io.arenadata.dtm.common.post.PostSqlActionType;
import io.vertx.core.Future;

public interface PostExecutor<T> {

    Future<Void> execute(T context);

    PostSqlActionType getPostActionType();
}
