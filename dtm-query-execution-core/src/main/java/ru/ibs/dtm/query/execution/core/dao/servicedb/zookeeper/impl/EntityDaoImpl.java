package ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.jackson.DatabindCodec;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.KeeperException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import ru.ibs.dtm.async.AsyncUtils;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.query.execution.core.dao.exception.DatamartNotExistsException;
import ru.ibs.dtm.query.execution.core.dao.exception.EntityAlreadyExistsException;
import ru.ibs.dtm.query.execution.core.dao.exception.EntityNotExistsException;
import ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import ru.ibs.dtm.query.execution.core.dto.metadata.DatamartEntity;
import ru.ibs.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;

import java.io.IOException;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

@Slf4j
@Repository
public class EntityDaoImpl implements EntityDao<Entity> {
    private final ZookeeperExecutor executor;
    private final String envPath;

    public EntityDaoImpl(ZookeeperExecutor executor, @Value("${core.env.name}") String systemName) {
        this.executor = executor;
        envPath = "/" + systemName;
    }

    @Override
    public void getEntitiesMeta(String datamartMnemonic, Handler<AsyncResult<List<DatamartEntity>>> resultHandler) {
        getEntityNamesByDatamart(datamartMnemonic)
            .onSuccess(names -> resultHandler.handle(
                Future.succeededFuture(
                    names.stream()
                        .map(name -> new DatamartEntity(null, name, datamartMnemonic))
                        .collect(Collectors.toList())
                )
            ))
            .onFailure(error -> resultHandler.handle(Future.failedFuture(error)));
    }

    @Override
    public Future<Void> createEntity(Entity entity) {
        try {
            byte[] entityData = DatabindCodec.mapper().writeValueAsBytes(entity);
            return executor.createPersistentPath(getTargetPath(entity), entityData)
                .compose(AsyncUtils::toEmptyVoidFuture)
                .otherwise(error -> {
                    String errMsg;
                    if (error instanceof KeeperException.NoNodeException) {
                        errMsg = String.format("Datamart [%s] not exists", entity.getSchema());
                        throw error(error, errMsg, DatamartNotExistsException::new);
                    } else if (error instanceof KeeperException.NodeExistsException) {
                        errMsg = String.format("Entity [%s] already exists", entity.getNameWithSchema());
                        throw error(error, errMsg, EntityAlreadyExistsException::new);
                    } else {
                        errMsg = String.format("Can't create entity [%s]", entity.getNameWithSchema());
                        throw error(error, errMsg, RuntimeException::new);
                    }
                });
        } catch (JsonProcessingException e) {
            return Future.failedFuture(
                error(e, String.format("Can't serialize entity [%s]", entity), RuntimeException::new)
            );
        }
    }

    @Override
    public Future<Void> updateEntity(Entity entity) {
        try {
            byte[] entityData = DatabindCodec.mapper().writeValueAsBytes(entity);
            return executor.setData(getTargetPath(entity), entityData, -1)
                .compose(AsyncUtils::toEmptyVoidFuture)
                .otherwise(error -> {
                    String errMsg;
                    if (error instanceof KeeperException.NoNodeException) {
                        errMsg = String.format("Entity [%s] not exists", entity.getNameWithSchema());
                        throw error(error, errMsg, EntityNotExistsException::new);
                    } else {
                        errMsg = String.format("Can't update entity [%s]", entity.getNameWithSchema());
                        throw error(error, errMsg, RuntimeException::new);
                    }
                });
        } catch (JsonProcessingException e) {
            return Future.failedFuture(
                error(e, String.format("Can't serialize entity [%s]", entity), RuntimeException::new)
            );
        }
    }

    @Override
    public Future<Void> deleteEntity(String datamartMnemonic, String entityName) {
        val nameWithSchema = getNameWithSchema(datamartMnemonic, entityName);
        return executor.delete(getTargetPath(datamartMnemonic, entityName), -1)
            .compose(AsyncUtils::toEmptyVoidFuture)
            .otherwise(error -> {
                String errMsg;
                if (error instanceof KeeperException.NoNodeException) {
                    errMsg = String.format("Entity [%s] not exists", nameWithSchema);
                    throw error(error, errMsg, EntityNotExistsException::new);
                } else {
                    errMsg = String.format("Can't delete entity [%s]", nameWithSchema);
                    throw error(error, errMsg, RuntimeException::new);
                }
            });
    }

    @Override
    public Future<Entity> getEntity(String datamartMnemonic, String entityName) {
        val nameWithSchema = getNameWithSchema(datamartMnemonic, entityName);
        return executor.getData(getTargetPath(datamartMnemonic, entityName))
            .map(entityData -> {
                try {
                    return DatabindCodec.mapper().readValue(entityData, Entity.class);
                } catch (IOException e) {
                    throw error(e,
                        String.format("Can't deserialize entity [%s]", nameWithSchema),
                        RuntimeException::new);
                }
            })
            .otherwise(error -> {
                String errMsg;
                if (error instanceof KeeperException.NoNodeException) {
                    errMsg = String.format("Entity [%s] not exists", nameWithSchema);
                    throw error(error, errMsg, EntityNotExistsException::new);
                } else {
                    errMsg = String.format("Can't get entity [%s]", nameWithSchema);
                    throw error(error, errMsg, RuntimeException::new);
                }
            });
    }

    @Override
    public Future<List<String>> getEntityNamesByDatamart(String datamartMnemonic) {
        return executor.getChildren(getEntitiesPath(datamartMnemonic))
            .onFailure(error -> {
                String errMsg;
                if (error instanceof KeeperException.NoNodeException) {
                    errMsg = String.format("Datamart [%s] not exists", datamartMnemonic);
                    throw error(error, errMsg, DatamartNotExistsException::new);
                } else {
                    errMsg = String.format("Can't get entity names by datamartMnemonic [%s]", datamartMnemonic);
                    throw error(error, errMsg, RuntimeException::new);
                }
            });
    }


    private RuntimeException error(Throwable error,
                                   String errMsg,
                                   BiFunction<String, Throwable, RuntimeException> errFunc) {
        log.error(errMsg, error);
        return errFunc.apply(errMsg, error);
    }

    @Override
    public String getTargetPath(Entity target) {
        return getTargetPath(target.getSchema(), target.getName());
    }

    public String getTargetPath(String datamartMnemonic, String entityName) {
        return String.format("%s/%s/entity/%s", envPath, datamartMnemonic, entityName);
    }

    public String getEntitiesPath(String datamartMnemonic) {
        return String.format("%s/%s/entity", envPath, datamartMnemonic);
    }

    public String getNameWithSchema(String datamartMnemonic, String entityName) {
        return datamartMnemonic + "." + entityName;
    }
}
