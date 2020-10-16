package ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.impl;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaDaoExecutor;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaServiceDaoExecutorHelper;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.WriteDeltaHotSuccessExecutor;
import ru.ibs.dtm.query.execution.core.dao.exception.delta.*;
import ru.ibs.dtm.query.execution.core.dto.delta.Delta;
import ru.ibs.dtm.query.execution.core.dto.delta.OkDelta;
import ru.ibs.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;

import java.time.LocalDateTime;
import java.util.Arrays;

@Slf4j
@Component
public class WriteDeltaHotSuccessExecutorImpl extends DeltaServiceDaoExecutorHelper implements WriteDeltaHotSuccessExecutor {

    public WriteDeltaHotSuccessExecutorImpl(ZookeeperExecutor executor,
                                            @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    @Override
    public Future<Long> execute(String datamart, LocalDateTime deltaHotDate) {
        val deltaStat = new Stat();
        Promise<Long> resultPromise = Promise.promise();
        val ctx = new DeltaContext();
        executor.getData(getDeltaPath(datamart), null, deltaStat)
            .map(bytes -> bytes == null ? new Delta() : deserializedDelta(bytes))
            .map(delta -> {
                ctx.setDelta(delta);
                return delta;
            })
            .compose(delta -> delta.getOk() == null ?
                Future.succeededFuture(delta) : createDeltaPaths(datamart, deltaHotDate, delta))
            .map(delta -> Delta.builder()
                .ok(OkDelta.builder()
                    .deltaDate(deltaHotDate == null ? LocalDateTime.now() : deltaHotDate)
                    .deltaNum(delta.getHot().getDeltaNum())
                    .cnFrom(delta.getHot().getCnFrom())
                    .cnTo(delta.getHot().getCnTo() == null ? 0 : delta.getHot().getCnTo())
                    .build())
                .build())
            .compose(delta -> executor.multi(getWriteDeltaHotSuccessOps(datamart, delta, deltaStat.getVersion())))
            .onSuccess(r -> {
                log.debug("write delta hot \"success\" by datamart[{}], deltaHotDate[{}] completed successfully", datamart, deltaHotDate);
                resultPromise.complete(ctx.getDelta().getHot().getDeltaNum());
            })
            .onFailure(error -> {
                val errMsg = String.format("can't write delta hot \"success\" by datamart[%s], deltaDate[%s]",
                    datamart,
                    deltaHotDate);
                log.error(errMsg, error);
                if (error instanceof KeeperException) {
                    if (error instanceof KeeperException.NotEmptyException) {
                        resultPromise.fail(new DeltaNotFinishedException(error));
                    } else if (error instanceof KeeperException.BadVersionException) {
                        resultPromise.fail(new DeltaAlreadyCommitedException(error));
                    } else {
                        resultPromise.fail(new DeltaException(errMsg, error));
                    }
                } else if (error instanceof DeltaException) {
                    resultPromise.fail(error);
                } else {
                    resultPromise.fail(new DeltaException(errMsg, error));
                }
            });

        return resultPromise.future();
    }

    private Future<Delta> createDeltaPaths(String datamart, LocalDateTime deltaHotDate, Delta delta) {
        if (delta.getHot() == null) {
            return Future.failedFuture(new DeltaHotNotStartedException());
        } else if (deltaHotDate != null && deltaHotDate.isBefore(delta.getOk().getDeltaDate())) {
            return Future.failedFuture(new InvalidDeltaDateException());
        } else {
            return createDeltaDatePath(datamart, delta)
                .map(delta)
                .otherwise(error -> {
                    if (error instanceof KeeperException.NodeExistsException) {
                        return delta;
                    } else {
                        throw new DeltaException("Can't write delta hot success", error);
                    }
                })
                .compose(r ->
                    createDeltaDateTimePath(datamart, delta.getOk())
                        .map(delta)
                        .otherwise(error -> {
                            if (error instanceof KeeperException.NodeExistsException) {
                                return r;
                            } else {
                                throw new DeltaException("Can't write delta hot success", error);
                            }
                        }))
                .compose(r ->
                    createDeltaDateNumPath(datamart, delta.getOk())
                        .map(delta)
                        .otherwise(error -> {
                            if (error instanceof KeeperException.NodeExistsException) {
                                return r;
                            } else {
                                throw new DeltaException("Can't write delta hot success", error);
                            }
                        }));
        }
    }

    private Future<String> createDeltaDatePath(String datamart, Delta delta) {
        val deltaDateTime = delta.getOk().getDeltaDate();
        val deltaDateTimePath = getDeltaDatePath(datamart, deltaDateTime.toLocalDate());
        return executor.createEmptyPersistentPath(deltaDateTimePath);
    }

    private Future<String> createDeltaDateTimePath(String datamart, OkDelta okDelta) {
        val deltaDateTime = okDelta.getDeltaDate();
        val deltaDateTimePath = getDeltaDateTimePath(datamart, deltaDateTime);
        return executor.createPersistentPath(deltaDateTimePath, serializedOkDelta(okDelta));
    }

    private Future<String> createDeltaDateNumPath(String datamart, OkDelta okDelta) {
        val deltaNum = okDelta.getDeltaNum();
        val deltaNumPath = getDeltaNumPath(datamart, deltaNum);
        return executor.createPersistentPath(deltaNumPath, serializedOkDelta(okDelta));
    }

    private Iterable<Op> getWriteDeltaHotSuccessOps(String datamart, Delta delta, int deltaVersion) {
        return Arrays.asList(
            Op.delete(getDatamartPath(datamart) + "/run", -1),
            Op.delete(getDatamartPath(datamart) + "/block", -1),
            Op.setData(getDeltaPath(datamart), serializedDelta(delta), deltaVersion)
        );
    }

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return WriteDeltaHotSuccessExecutor.class;
    }

}
