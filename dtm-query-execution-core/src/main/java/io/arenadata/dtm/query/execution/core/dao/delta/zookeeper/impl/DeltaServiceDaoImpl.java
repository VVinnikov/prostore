package io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.impl;

import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaDaoExecutorRepository;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.executor.*;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaWriteOp;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaWriteOpRequest;
import io.arenadata.dtm.query.execution.core.dto.delta.HotDelta;
import io.arenadata.dtm.query.execution.core.dto.delta.OkDelta;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class DeltaServiceDaoImpl implements DeltaServiceDao, DeltaDaoExecutorRepository {
    private final Map<Class<? extends DeltaDaoExecutor>, DeltaDaoExecutor> executorMap;

    public DeltaServiceDaoImpl() {
        executorMap = new HashMap<>();
    }

    @Override
    public Future<Long> writeNewDeltaHot(String datamart) {
        return writeNewDeltaHot(datamart, null);
    }

    @Override
    public Future<Long> writeNewDeltaHot(String datamart, Long deltaHotNum) {
        return getExecutor(WriteNewDeltaHotExecutor.class).execute(datamart, deltaHotNum);
    }

    @Override
    public Future<LocalDateTime> writeDeltaHotSuccess(String datamart) {
        return writeDeltaHotSuccess(datamart, null);
    }

    @Override
    public Future<LocalDateTime> writeDeltaHotSuccess(String datamart, LocalDateTime deltaHotDate) {
        return getExecutor(WriteDeltaHotSuccessExecutor.class).execute(datamart, deltaHotDate);
    }

    @Override
    public Future<Void> writeDeltaError(String datamart, Long deltaHotNum) {
        return getExecutor(WriteDeltaErrorExecutor.class).execute(datamart, deltaHotNum);
    }

    @Override
    public Future<Void> deleteDeltaHot(String datamart) {
        return getExecutor(DeleteDeltaHotExecutor.class).execute(datamart);
    }

    @Override
    public Future<Long> writeNewOperation(DeltaWriteOpRequest operation) {
        return getExecutor(WriteNewOperationExecutor.class).execute(operation);
    }

    @Override
    public Future<Void> writeOperationSuccess(String datamart, long synCn) {
        return getExecutor(WriteOperationSuccessExecutor.class).execute(datamart, synCn);
    }

    @Override
    public Future<Void> writeOperationError(String datamart, long synCn) {
        return getExecutor(WriteOperationErrorExecutor.class).execute(datamart, synCn);
    }

    @Override
    public Future<Void> deleteWriteOperation(String datamart, long synCn) {
        return getExecutor(DeleteWriteOperationExecutor.class).execute(datamart, synCn);
    }

    @Override
    public Future<OkDelta> getDeltaByNum(String datamart, long num) {
        return getExecutor(GetDeltaByNumExecutor.class).execute(datamart, num);
    }

    @Override
    public Future<OkDelta> getDeltaByDateTime(String datamart, LocalDateTime dateTime) {
        return getExecutor(GetDeltaByDateTimeExecutor.class).execute(datamart, dateTime);
    }

    @Override
    public Future<OkDelta> getDeltaOk(String datamart) {
        return getExecutor(GetDeltaOkExecutor.class).execute(datamart);
    }

    @Override
    public Future<HotDelta> getDeltaHot(String datamart) {
        return getExecutor(GetDeltaHotExecutor.class).execute(datamart);
    }

    @Override
    public Future<List<DeltaWriteOp>> getDeltaWriteOperations(String datamart) {
        return getExecutor(GetDeltaWriteOperationsExecutor.class).execute(datamart);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends DeltaDaoExecutor> T getExecutor(Class<T> executorInterface) {
        return (T) executorMap.get(executorInterface);
    }

    @Override
    public <T extends DeltaDaoExecutor> void addExecutor(T executor) {
        executorMap.put(executor.getExecutorInterface(), executor);
    }

}
