package ru.ibs.dtm.query.execution.plugin.adb.factory;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import ru.ibs.dtm.common.model.ddl.Entity;

/**
 * Metadata executor
 */
public interface MetadataFactory {
    /**
     * Apply the physical model to the database
     *
     * @param entity  physical model
     * @param handler Handler
     */
    void apply(Entity entity, Handler<AsyncResult<Void>> handler);

    /**
     * Remove physical model from database
     *
     * @param entity  physical model
     * @param handler Handler
     */
    void purge(Entity entity, Handler<AsyncResult<Void>> handler);
}
