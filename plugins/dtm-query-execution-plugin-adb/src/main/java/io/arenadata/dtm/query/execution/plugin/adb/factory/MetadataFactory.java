package io.arenadata.dtm.query.execution.plugin.adb.factory;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

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

    /**
     * Remove physical model from database
     *
     * @param entity  physical model
     * @param handler Handler
     */
    void purge(Entity entity, Handler<AsyncResult<Void>> handler);
}
