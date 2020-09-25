package ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.springframework.beans.factory.annotation.Value;
import ru.ibs.dtm.async.AsyncUtils;
import ru.ibs.dtm.query.execution.core.dao.exception.DatamartAlreadyExistsException;
import ru.ibs.dtm.query.execution.core.dao.exception.DatamartNotExistsException;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import ru.ibs.dtm.query.execution.core.dto.metadata.DatamartInfo;
import ru.ibs.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

@Slf4j
public class DatamartDaoImpl implements DatamartDao {
    private static final int CREATE_DATAMART_OP_INDEX = 0;
    private static final byte[] EMPTY_DATA = new byte[0];
    private final ZookeeperExecutor executor;
    private final String envPath;

    public DatamartDaoImpl(ZookeeperExecutor executor, @Value("${core.env.name}") String systemName) {
        this.executor = executor;
        envPath = "/" + systemName;
    }

    @Override
    public Future<Void> createDatamart(String name) {
        return executor.createEmptyPersistentPath(envPath)
            .otherwise(error -> {
                if (error instanceof KeeperException.NodeExistsException) {
                    return envPath;
                } else {
                    throw error(error,
                        String.format("Can't create datamart [%s]", name),
                        RuntimeException::new);
                }
            })
            .compose(r -> executor.multi(getCreateDatamartOps(getTargetPath(name))))
            .otherwise(error -> {
                if (error instanceof KeeperException.NodeExistsException) {
                    if (isDatamartExists((KeeperException) error)) {
                        throw error(error,
                            String.format("Datamart [%s] already exists!", name),
                            DatamartAlreadyExistsException::new);
                    }
                }
                throw error(error,
                    String.format("Can't create datamart [%s]", name),
                    RuntimeException::new);
            })
            .compose(AsyncUtils::toEmptyVoidFuture)
            .onSuccess(s -> log.info("Datamart [{}] successfully created", name));
    }

    private List<Op> getCreateDatamartOps(String datamartPath) {
        return Arrays.asList(
            Op.create(datamartPath, EMPTY_DATA, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
            Op.create(datamartPath + "/entity", EMPTY_DATA, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
            Op.create(datamartPath + "/delta", EMPTY_DATA, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
    }

    private boolean isDatamartExists(KeeperException error) {
        List<OpResult> results = error.getResults() == null ? Collections.emptyList() : error.getResults();
        return results.size() > 0 && results.get(CREATE_DATAMART_OP_INDEX) instanceof OpResult.ErrorResult;
    }

    @Override
    public void getDatamartMeta(Handler<AsyncResult<List<DatamartInfo>>> resultHandler) {
        getDatamarts()
            .onSuccess(names -> {
                resultHandler.handle(Future.succeededFuture(
                    names.stream()
                        .map(DatamartInfo::new)
                        .collect(Collectors.toList())
                ));
            })
            .onFailure(error -> resultHandler.handle(Future.failedFuture(error)));
    }

    @Override
    public Future<List<String>> getDatamarts() {
        return executor.getChildren(envPath)
            .otherwise(error -> {
                if (error instanceof KeeperException.NoNodeException) {
                    throw error(error,
                        String.format("Env [%s] not exists", envPath),
                        RuntimeException::new);
                } else {
                    throw error(error,
                        "Can't get datamarts",
                        RuntimeException::new);
                }
            });
    }

    @Override
    public Future<?> getDatamart(String name) {
        return executor.getData(getTargetPath(name))
            .otherwise(error -> {
                if (error instanceof KeeperException.NoNodeException) {
                    throw error(error,
                        String.format("Datamart [%s] not exists", name),
                        DatamartNotExistsException::new);
                } else {
                    throw error(error,
                        String.format("Can't get datamarts [%s]", name),
                        RuntimeException::new);
                }
            });
    }

    @Override
    public Future<Boolean> existsDatamart(String name) {
        return executor.exists(getTargetPath(name));
    }

    @Override
    public Future<Void> deleteDatamart(String name) {
        return executor.deleteRecursive(getTargetPath(name))
            .otherwise(error -> {
                if (error instanceof IllegalArgumentException) {
                    throw error(error,
                        String.format("Datamart [%s] does not exists!", name),
                        DatamartNotExistsException::new);
                } else {
                    throw error(error,
                        String.format("Can't delete datamarts [%s]", name),
                        RuntimeException::new);
                }
            })
            .onSuccess(s -> log.info("Datamart [{}] successfully removed", name));
    }

    private RuntimeException error(Throwable error,
                                   String errMsg,
                                   BiFunction<String, Throwable, RuntimeException> errFunc) {
        log.error(errMsg, error);
        return errFunc.apply(errMsg, error);
    }

    @Override
    public String getTargetPath(String target) {
        return String.format("%s/%s", envPath, target);
    }
}
