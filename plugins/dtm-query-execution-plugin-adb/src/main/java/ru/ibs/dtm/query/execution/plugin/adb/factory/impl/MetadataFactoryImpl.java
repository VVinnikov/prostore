package ru.ibs.dtm.query.execution.plugin.adb.factory.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MetadataFactory;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;

@Slf4j
@Service
@RequiredArgsConstructor
@Deprecated
public class MetadataFactoryImpl implements MetadataFactory {

    private final AdbQueryExecutor adbQueryExecutor;
    private final MetadataSqlFactory sqlFactory;

    @Override
    public void apply(Entity entity, Handler<AsyncResult<Void>> handler) {
        String dropSql = sqlFactory.createDropTableScript(entity);
        adbQueryExecutor.executeUpdate(dropSql, ar -> {
            if (ar.succeeded()) {
                String createSql = sqlFactory.createTableScripts(entity);
                adbQueryExecutor.executeUpdate(createSql, handler);
            } else {
                log.error("Error executing the apply method of the ADB plugin", ar.cause());
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @Override
    public void purge(Entity entity, Handler<AsyncResult<Void>> handler) {
        String dropSql = sqlFactory.createDropTableScript(entity);
        adbQueryExecutor.executeUpdate(dropSql, handler);
    }
}
