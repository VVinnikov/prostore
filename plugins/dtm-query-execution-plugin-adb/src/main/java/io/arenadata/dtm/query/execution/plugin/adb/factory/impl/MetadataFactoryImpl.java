package io.arenadata.dtm.query.execution.plugin.adb.factory.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MetadataFactory;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
@Deprecated
public class MetadataFactoryImpl implements MetadataFactory {

    private final AdbQueryExecutor adbQueryExecutor;
    private final MetadataSqlFactory sqlFactory;

    @Override
    public void purge(Entity entity, Handler<AsyncResult<Void>> handler) {
        String dropSql = sqlFactory.createDropTableScript(entity);
        adbQueryExecutor.executeUpdate(dropSql, handler);
    }
}
