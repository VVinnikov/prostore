package io.arenadata.dtm.query.execution.core.delta.repository.executor.impl;

import io.arenadata.dtm.query.execution.core.delta.repository.executor.DeltaDaoExecutor;
import io.arenadata.dtm.query.execution.core.delta.repository.executor.DeltaServiceDaoExecutorHelper;
import io.arenadata.dtm.query.execution.core.delta.repository.executor.WriteNewOperationExecutor;
import io.arenadata.dtm.query.execution.core.delta.exception.DeltaClosedException;
import io.arenadata.dtm.query.execution.core.delta.exception.DeltaException;
import io.arenadata.dtm.query.execution.core.delta.exception.TableBlockedException;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaWriteOp;
import io.arenadata.dtm.query.execution.core.delta.dto.DeltaWriteOpRequest;
import io.arenadata.dtm.query.execution.core.base.service.zookeeper.ZookeeperExecutor;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Slf4j
@Component
public class WriteNewOperationExecutorImpl extends DeltaServiceDaoExecutorHelper implements WriteNewOperationExecutor {

    private static final int CREATE_OP_PATH_INDEX = 1;

    @Autowired
    public WriteNewOperationExecutorImpl(ZookeeperExecutor executor,
                                         @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    @Override
    public Future<Long> execute(DeltaWriteOpRequest request) {
        Promise<Long> resultPromise = Promise.promise();
        executor.getData(getDeltaPath(request.getDatamart()))
            .map(bytes -> {
                val delta = deserializedDelta(bytes);
                if (delta.getHot() == null) {
                    throw new DeltaClosedException();
                }
                return delta.getHot().getCnFrom();
            })
            .compose(cnFrom -> executor.multi(getWriteNewOps(request, cnFrom))
                .map(result -> getSysCn(cnFrom, result)))
            .onSuccess(sysCn -> {
                log.debug("Write new delta operation by datamart[{}] completed successfully: sysCn[{}]", request, sysCn);
                resultPromise.complete(sysCn);
            })
            .onFailure(error -> {
                val errMsg = String.format("Can't write new operation on datamart[%s]",
                    request.getDatamart());
                if (error instanceof KeeperException.NodeExistsException) {
                    resultPromise.fail(new TableBlockedException(request.getTableName(), error));
                } else if (error instanceof DeltaException) {
                    resultPromise.fail(error);
                } else {
                    resultPromise.fail(new DeltaException(errMsg, error));
                }
            });
        return resultPromise.future();
    }

    private Long getSysCn(Long cnFrom, List<OpResult> result) {
        if (result.size() == 2 && result.get(CREATE_OP_PATH_INDEX) instanceof OpResult.CreateResult) {
            try {
                val opPath = ((OpResult.CreateResult) result.get(CREATE_OP_PATH_INDEX)).getPath();
                val opNumber = Long.parseLong(opPath.substring(opPath.lastIndexOf("/") + 1));
                return opNumber + cnFrom;
            } catch (NumberFormatException e) {
                throw new DeltaException("Can't get op number", e);
            }
        } else {
            throw new DeltaException("Can't create sequential op node");
        }
    }

    private Iterable<Op> getWriteNewOps(DeltaWriteOpRequest request, long cnFrom) {
        return Arrays.asList(
            createDatamartNodeOp(getDatamartPath(request.getDatamart()), "/block/" + request.getTableName()),
            Op.create(getDatamartPath(request.getDatamart()) + "/run/",
                getWriteOpData(request, cnFrom),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL)
        );
    }

    private byte[] getWriteOpData(DeltaWriteOpRequest request, long cnFrom) {
        val deltaWriteOp = DeltaWriteOp.builder()
            .cnFrom(cnFrom)
            .tableNameExt(request.getTableNameExt())
            .tableName(request.getTableName())
            .query(request.getQuery())
            .status(0)
            .build();
        return serializeDeltaWriteOp(deltaWriteOp);
    }

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return WriteNewOperationExecutor.class;
    }
}
