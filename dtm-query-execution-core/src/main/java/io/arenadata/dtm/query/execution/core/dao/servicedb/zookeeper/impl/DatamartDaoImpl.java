package io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl;

import io.arenadata.dtm.async.AsyncUtils;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dto.delta.Delta;
import io.arenadata.dtm.query.execution.core.dto.metadata.DatamartInfo;
import io.arenadata.dtm.query.execution.core.exception.datamart.DatamartAlreadyExistsException;
import io.arenadata.dtm.query.execution.core.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;
import io.vertx.core.Future;
import io.vertx.core.json.jackson.DatabindCodec;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Repository
public class DatamartDaoImpl implements DatamartDao {
    private static final int CREATE_DATAMART_OP_INDEX = 0;
    private static final byte[] EMPTY_DATA = null;
    private final ZookeeperExecutor executor;
    private final String envPath;

    @Autowired
    public DatamartDaoImpl(@Qualifier("zookeeperExecutor") ZookeeperExecutor executor,
                           @Value("${core.env.name}") String systemName) {
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
                        throw new DtmException(
                                String.format("Can't create datamart [%s]", name),
                                error);
                    }
                })
                .compose(r -> executor.multi(getCreateDatamartOps(getTargetPath(name))))
                .otherwise(error -> {
                    if (error instanceof KeeperException.NodeExistsException) {
                        if (isDatamartExists((KeeperException) error)) {
                            throw new DatamartAlreadyExistsException(name);
                        }
                    }
                    throw new DtmException(String.format("Can't create datamart [%s]",
                            name),
                            error);
                })
                .compose(AsyncUtils::toEmptyVoidFuture)
                .onSuccess(s -> log.info("Datamart [{}] successfully created", name));
    }

    private List<Op> getCreateDatamartOps(String datamartPath) {
        byte[] deltaData;
        try {
            deltaData = DatabindCodec.mapper().writeValueAsBytes(new Delta());
        } catch (Exception ex) {
            throw new RuntimeException("Can't serialize delta");
        }
        return Arrays.asList(
                Op.create(datamartPath, EMPTY_DATA, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                createDatamartNodeOp(datamartPath, "/entity"),
                Op.create(datamartPath + "/delta", deltaData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
                createDatamartNodeOp(datamartPath, "/delta/num"),
                createDatamartNodeOp(datamartPath, "/delta/date")
        );
    }

    private Op createDatamartNodeOp(String datamartPath, String nodeName) {
        return Op.create(datamartPath + nodeName, EMPTY_DATA, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    private boolean isDatamartExists(KeeperException error) {
        List<OpResult> results = error.getResults() == null ? Collections.emptyList() : error.getResults();
        return results.size() > 0 && results.get(CREATE_DATAMART_OP_INDEX) instanceof OpResult.ErrorResult;
    }

    @Override
    public Future<List<DatamartInfo>> getDatamartMeta() {
        return getDatamarts()
                .map(names -> names.stream()
                        .map(DatamartInfo::new)
                        .collect(Collectors.toList()
                        ));
    }

    @Override
    public Future<List<String>> getDatamarts() {
        return executor.getChildren(envPath)
                .otherwise(error -> {
                    if (error instanceof KeeperException.NoNodeException) {
                        throw new DtmException(
                                String.format("Env [%s] not exists", envPath),
                                error);
                    } else {
                        throw new DtmException("Can't get datamarts", error);
                    }
                });
    }

    @Override
    public Future<?> getDatamart(String name) {
        return executor.getData(getTargetPath(name))
                .otherwise(error -> {
                    if (error instanceof KeeperException.NoNodeException) {
                        throw new DatamartNotExistsException(name);
                    } else {
                        throw new DtmException(
                                String.format("Can't get datamarts [%s]", name),
                                error);
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
                        throw new DatamartNotExistsException(name);
                    } else {
                        throw new DtmException(String.format("Can't delete datamarts [%s]",
                                name),
                                error);
                    }
                })
                .onSuccess(s -> log.info("Datamart [{}] successfully removed", name));
    }

    @Override
    public String getTargetPath(String target) {
        return String.format("%s/%s", envPath, target);
    }
}
