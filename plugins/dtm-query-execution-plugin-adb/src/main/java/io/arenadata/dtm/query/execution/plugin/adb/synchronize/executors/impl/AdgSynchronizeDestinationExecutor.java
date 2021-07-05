package io.arenadata.dtm.query.execution.plugin.adb.synchronize.executors.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.execution.plugin.adb.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adb.synchronize.executors.SynchronizeDestinationExecutor;
import io.arenadata.dtm.query.execution.plugin.adb.synchronize.factory.SynchronizeSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.synchronize.service.PrepareQueriesOfChangesService;
import io.arenadata.dtm.query.execution.plugin.adb.synchronize.service.PrepareRequestOfChangesRequest;
import io.arenadata.dtm.query.execution.plugin.adb.synchronize.service.PrepareRequestOfChangesResult;
import io.arenadata.dtm.query.execution.plugin.api.service.shared.adg.AdgSharedService;
import io.arenadata.dtm.query.execution.plugin.api.shared.adg.AdgSharedPrepareStagingRequest;
import io.arenadata.dtm.query.execution.plugin.api.shared.adg.AdgSharedTransferDataRequest;
import io.arenadata.dtm.query.execution.plugin.api.synchronize.SynchronizeRequest;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class AdgSynchronizeDestinationExecutor implements SynchronizeDestinationExecutor {
    private final PrepareQueriesOfChangesService prepareQueriesOfChangesService;
    private final DatabaseExecutor databaseExecutor;
    private final SynchronizeSqlFactory synchronizeSqlFactory;
    private final AdgSharedService adgSharedService;

    public AdgSynchronizeDestinationExecutor(PrepareQueriesOfChangesService prepareQueriesOfChangesService,
                                             DatabaseExecutor databaseExecutor,
                                             SynchronizeSqlFactory synchronizeSqlFactory,
                                             AdgSharedService adgSharedService) {
        this.prepareQueriesOfChangesService = prepareQueriesOfChangesService;
        this.databaseExecutor = databaseExecutor;
        this.synchronizeSqlFactory = synchronizeSqlFactory;
        this.adgSharedService = adgSharedService;
    }

    @Override
    public Future<Long> execute(SynchronizeRequest synchronizeRequest) {
        return Future.future(promise -> {
            log.info("Started [ADB->ADG][{}] synchronization, deltaNum: {}", synchronizeRequest.getRequestId(), synchronizeRequest.getDeltaNumToBe());
            if (synchronizeRequest.getDatamarts().size() > 1) {
                promise.fail(new DtmException(String.format("Can't synchronize [ADB->ADG][%s] with multiple datamarts: %s",
                        synchronizeRequest.getEntity().getName(), synchronizeRequest.getDatamarts())));
                return;
            }

            prepareQueriesOfChangesService.prepare(new PrepareRequestOfChangesRequest(synchronizeRequest.getDatamarts(), synchronizeRequest.getEnvName(),
                    synchronizeRequest.getDeltaNumToBe(), synchronizeRequest.getViewQuery(), synchronizeRequest.getEntity()))
                    .compose(requestOfChanges -> synchronize(requestOfChanges, synchronizeRequest))
                    .onComplete(result -> executeDropExternalTable(synchronizeRequest.getDatamartMnemonic(), synchronizeRequest.getEntity())
                            .onComplete(dropResult -> {
                                if (dropResult.failed()) {
                                    log.error("Could not drop external table [{}]", synchronizeRequest.getEntity().getNameWithSchema(), dropResult.cause());
                                }

                                if (result.succeeded()) {
                                    promise.complete(synchronizeRequest.getDeltaNumToBe());
                                } else {
                                    promise.fail(result.cause());
                                }
                            }));
        });
    }

    private Future<Void> synchronize(PrepareRequestOfChangesResult requestOfChanges, SynchronizeRequest synchronizeRequest) {
        return executeDropExternalTable(synchronizeRequest.getDatamartMnemonic(), synchronizeRequest.getEntity())
                .compose(r -> executeCreateExternalTable(synchronizeRequest.getEnvName(), synchronizeRequest.getDatamartMnemonic(), synchronizeRequest.getEntity()))
                .compose(r -> truncateSpace(synchronizeRequest))
                .compose(r -> insertChanges(requestOfChanges, synchronizeRequest))
                .compose(r -> transferSpaceChanges(synchronizeRequest));
    }

    private Future<Void> truncateSpace(SynchronizeRequest synchronizeRequest) {
        return adgSharedService.prepareStaging(new AdgSharedPrepareStagingRequest(synchronizeRequest.getEnvName(), synchronizeRequest.getDatamartMnemonic(),
                synchronizeRequest.getEntity()));
    }

    private Future<List<Map<String, Object>>> insertChanges(PrepareRequestOfChangesResult requestOfChanges, SynchronizeRequest synchronizeRequest) {
        return executeInsertIntoExternalTable(synchronizeRequest.getDatamartMnemonic(), synchronizeRequest.getEntity(), requestOfChanges.getDeletedRecordsQuery())
                .compose(ar -> executeInsertIntoExternalTable(synchronizeRequest.getDatamartMnemonic(), synchronizeRequest.getEntity(), requestOfChanges.getNewRecordsQuery()));
    }

    private Future<Void> transferSpaceChanges(SynchronizeRequest synchronizeRequest) {
        return adgSharedService.transferData(new AdgSharedTransferDataRequest(synchronizeRequest.getEnvName(), synchronizeRequest.getDatamartMnemonic(),
                synchronizeRequest.getEntity(), synchronizeRequest.getDeltaNumToBeCnTo()));
    }

    private Future<List<Map<String, Object>>> executeDropExternalTable(String datamart, Entity entity) {
        return Future.future(event -> {
            String dropSql = synchronizeSqlFactory.dropExternalTable(datamart, entity);
            databaseExecutor.execute(dropSql).onComplete(event);
        });
    }

    private Future<List<Map<String, Object>>> executeCreateExternalTable(String env, String datamart, Entity entity) {
        return Future.future(event -> {
            String createSql = synchronizeSqlFactory.createExternalTable(env, datamart, entity);
            databaseExecutor.execute(createSql).onComplete(event);
        });
    }

    private Future<List<Map<String, Object>>> executeInsertIntoExternalTable(String datamart, Entity entity, String query) {
        return Future.future(event -> {
            String insertIntoSql = synchronizeSqlFactory.insertIntoExternalTable(datamart, entity, query);
            databaseExecutor.execute(insertIntoSql).onComplete(event);
        });
    }

    @Override
    public SourceType getDestination() {
        return SourceType.ADG;
    }
}
