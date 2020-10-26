package io.arenadata.dtm.query.execution.core.service.edml.impl;

import io.arenadata.dtm.common.exception.CrashException;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.factory.RollbackRequestContextFactory;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.core.service.edml.EdmlUploadFailedExecutor;
import io.arenadata.dtm.query.execution.plugin.api.edml.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.rollback.RollbackRequestContext;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class EdmlUploadFailedExecutorImpl implements EdmlUploadFailedExecutor {

    private final DeltaServiceDao deltaServiceDao;
    private final RollbackRequestContextFactory rollbackRequestContextFactory;
    private final DataSourcePluginService dataSourcePluginService;

    @Autowired
    public EdmlUploadFailedExecutorImpl(DeltaServiceDao deltaServiceDao,
                                        RollbackRequestContextFactory rollbackRequestContextFactory,
                                        DataSourcePluginService dataSourcePluginService) {
        this.deltaServiceDao = deltaServiceDao;
        this.rollbackRequestContextFactory = rollbackRequestContextFactory;
        this.dataSourcePluginService = dataSourcePluginService;
    }

    @Override
    public Future<Void> execute(EdmlRequestContext context) {
        return Future.future(promise -> eraseWriteOp(context)
                .compose(v -> deltaServiceDao.deleteWriteOperation(context.getSourceTable().getSchemaName(),
                        context.getSysCn()))
                .setHandler(promise));
    }

    private Future<Void> eraseWriteOp(EdmlRequestContext context) {
        return Future.future(rbPromise -> {
            List<Future> futures = new ArrayList<>();
            final RollbackRequestContext rollbackRequestContext =
                    rollbackRequestContextFactory.create(context);
            dataSourcePluginService.getSourceTypes().forEach(sourceType ->
                    futures.add(Future.future(p -> dataSourcePluginService.rollback(
                            sourceType,
                            rollbackRequestContext,
                            ar -> {
                                if (ar.succeeded()) {
                                    log.debug("Rollback data in plugin [{}], datamart [{}], " +
                                                    "table [{}], sysCn [{}] finished successfully",
                                            sourceType,
                                            context.getEntity().getSchema(),
                                            context.getTargetTable().getTableName(),
                                            context.getSysCn());
                                    p.complete();
                                } else {
                                    log.error("Error rollback data in plugin [{}], " +
                                                    "datamart [{}], table [{}], sysCn [{}]",
                                            sourceType,
                                            context.getEntity().getSchema(),
                                            context.getTargetTable().getTableName(),
                                            context.getSysCn(),
                                            ar.cause());
                                    p.fail(ar.cause());
                                }
                            }))));
            CompositeFuture.join(futures).setHandler(ar -> {
                if (ar.succeeded()) {
                    rbPromise.complete();
                } else {
                    rbPromise.fail(new CrashException("Error in rolling back data", ar.cause()));
                }
            });
        });
    }
}
