package ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.impl;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.exception.CrashException;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaDaoExecutor;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.DeltaServiceDaoExecutorHelper;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.executor.WriteOperationSuccessExecutor;
import ru.ibs.dtm.query.execution.core.dao.exception.delta.DeltaException;
import ru.ibs.dtm.query.execution.core.dao.exception.delta.DeltaNotExistException;
import ru.ibs.dtm.query.execution.core.dto.delta.Delta;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaWriteOp;
import ru.ibs.dtm.query.execution.core.dto.delta.HotDelta;
import ru.ibs.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Component
public class WriteOperationSuccessExecutorImpl extends DeltaServiceDaoExecutorHelper implements WriteOperationSuccessExecutor {

    public WriteOperationSuccessExecutorImpl(ZookeeperExecutor executor,
                                             @Value("${core.env.name}") String envName) {
        super(executor, envName);
    }

    @Override
    public Future<Void> execute(String datamart, long sysCn) {
        Promise<Void> resultPromise = Promise.promise();
        val deltaStat = new Stat();
        val writeOpStat = new Stat();
        val ctx = new WriteOpContext(datamart, sysCn);
        getDatamartDeltaData(datamart, deltaStat)
            .map(bytes -> {
                val delta = deserializedDelta(bytes);
                if (delta.getHot() == null) {
                    throw new CrashException("Delta hot not exists", new DeltaNotExistException());
                }
                ctx.setDelta(delta);
                ctx.setOpNum(sysCn - delta.getHot().getCnFrom());
                ctx.setDeltaVersion(deltaStat.getVersion());
                return ctx;
            })
            .compose(opNum -> getWriteOpData(datamart, writeOpStat, ctx))
            .map(this::deserializeDeltaWriteOp)
            .map(writeOp -> {
                ctx.setWriteOp(writeOp);
                return writeOp;
            })
            .compose(writeOp -> executor.getChildren(getDatamartPath(datamart) + "/run"))
            .map(opPaths -> updateDeltaHot(ctx, opPaths))
            .compose(delta -> executor.multi(getUpdateOperationNodesOps(ctx)))
            .onSuccess(delta -> {
                log.debug("write delta operation \"success\" by datamart[{}], sysCn[{}] completed successfully", datamart, sysCn);
                resultPromise.complete();
            })
            .onFailure(error -> {
                val errMsg = String.format("can't write operation error by datamart[%s], sysCn[%d]",
                    datamart,
                    sysCn);
                log.error(errMsg, error);
                resultPromise.fail(new DeltaException(errMsg, error));
            });
        return resultPromise.future();
    }

    private Future<byte[]> getWriteOpData(String datamart, Stat writeOpStat, WriteOpContext ctx) {
        return getZkNodeData(writeOpStat, getWriteOpPath(datamart, ctx.getOpNum()));
    }

    private Future<byte[]> getDatamartDeltaData(String datamart, Stat deltaStat) {
        return getZkNodeData(deltaStat, getDeltaPath(datamart));
    }

    private Future<byte[]> getZkNodeData(Stat stat, String nodePath) {
        return executor.getData(nodePath, null, stat);
    }

    private Iterable<Op> getUpdateOperationNodesOps(WriteOpContext ctx) {
        return Arrays.asList(
            Op.delete(getWriteOpPath(ctx.getDatamart(), ctx.getOpNum()), ctx.getWriteOpVersion()),
            Op.delete(getBlockTablePath(ctx), -1),
            Op.setData(getDeltaPath(ctx.getDatamart()), serializedDelta(ctx.getDelta()), ctx.getDeltaVersion())
        );
    }

    private String getBlockTablePath(WriteOpContext ctx) {
        return getDatamartPath(ctx.getDatamart()) + "/block/" + ctx.getWriteOp().getTableName();
    }

    private HotDelta updateDeltaHot(WriteOpContext ctx, List<String> opPaths) {
        List<Long> opNums = opPaths.stream()
            .map(path -> Long.parseLong(path.substring(path.lastIndexOf("/") + 1)))
            .collect(Collectors.toList());
        val hotDelta = ctx.getDelta().getHot();
        val opN = ctx.getSysCn() - hotDelta.getCnFrom();
        val opMax = hotDelta.getCnMax() - hotDelta.getCnFrom();
        val cnMax = Math.max(ctx.getSysCn(), hotDelta.getCnMax());
        getLastOpDoneInSequence(opN, opMax, opNums).ifPresent(lastOpDone -> {
            val cnTo = hotDelta.getCnFrom() + lastOpDone;
            hotDelta.setCnTo(cnTo);
        });
        hotDelta.setCnMax(cnMax);
        return hotDelta;
    }

    private Optional<Long> getLastOpDoneInSequence(long opN, long opMax, List<Long> opNums) {
        if (opNums.stream().anyMatch(op -> op < opN)) {
            return Optional.empty();
        } else {
            return Optional.of(
                opNums.stream()
                    .filter(op -> op > opN)
                    .min(Comparator.naturalOrder())
                    .orElse(Math.max(opN, opMax))
            );
        }
    }

    @Override
    public Class<? extends DeltaDaoExecutor> getExecutorInterface() {
        return WriteOperationSuccessExecutor.class;
    }

    @Data
    private static final class WriteOpContext {
        private final String datamart;
        private final long sysCn;
        private DeltaWriteOp writeOp;
        private int writeOpVersion;
        private int deltaVersion;
        private Delta delta;
        private long opNum;
    }
}
