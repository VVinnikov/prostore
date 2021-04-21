package io.arenadata.dtm.query.execution.core.check.service.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.check.dto.CheckContext;
import io.arenadata.dtm.query.execution.core.base.exception.datamart.DatamartNotExistsException;
import io.arenadata.dtm.query.execution.core.check.factory.CheckQueryResultFactory;
import io.arenadata.dtm.query.execution.core.check.service.CheckExecutor;
import io.arenadata.dtm.query.execution.core.plugin.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckException;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckTableRequest;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.apache.calcite.util.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service("checkDatabaseExecutor")
public class CheckDatabaseExecutor implements CheckExecutor {

    private final DataSourcePluginService dataSourcePluginService;
    private final EntityDao entityDao;
    private final DatamartDao datamartDao;
    private final CheckQueryResultFactory queryResultFactory;

    @Autowired
    public CheckDatabaseExecutor(DataSourcePluginService dataSourcePluginService,
                                 EntityDao entityDao,
                                 DatamartDao datamartDao,
                                 CheckQueryResultFactory queryResultFactory) {
        this.dataSourcePluginService = dataSourcePluginService;
        this.entityDao = entityDao;
        this.datamartDao = datamartDao;
        this.queryResultFactory = queryResultFactory;
    }

    @Override
    public Future<QueryResult> execute(CheckContext context) {
        String datamartMnemonic = context.getRequest().getQueryRequest().getDatamartMnemonic();
        return datamartExists(datamartMnemonic)
                .compose(exist -> exist ? entityDao.getEntityNamesByDatamart(datamartMnemonic)
                        : Future.failedFuture(new DatamartNotExistsException(datamartMnemonic)))
                .compose(names -> getEntities(names, datamartMnemonic))
                .compose(entities -> checkEntities(entities, context))
                .map(queryResultFactory::create);
    }

    private Future<Boolean> datamartExists(String datamart) {
        return Future.future(promise -> datamartDao.getDatamart(datamart)
                .onSuccess(success -> promise.complete(true))
                .onFailure(error -> promise.complete(false)));
    }

    private Future<List<Entity>> getEntities(List<String> entityNames, String datamartMnemonic) {
        return Future.future(promise -> CompositeFuture.join(
                entityNames.stream()
                        .map(name -> entityDao.getEntity(datamartMnemonic, name))
                        .collect(Collectors.toList())
        )
                .onSuccess(result -> promise.complete(result.list()))
                .onFailure(promise::fail));
    }

    private Future<String> checkEntities(List<Entity> entities, CheckContext context) {
        return Future.future(promise -> CompositeFuture.join(dataSourcePluginService.getSourceTypes()
                .stream()
                .map(type -> checkEntitiesBySourceType(entities, context, type))
                .collect(Collectors.toList()))
                .onSuccess(result -> {
                    List<Pair<SourceType, List<String>>> list = result.list();
                    if (list.stream().allMatch(pair -> pair.getValue().isEmpty())) {
                        promise.complete(String.format("Datamart %s (%s) is ok.",
                                context.getRequest().getQueryRequest().getDatamartMnemonic(),
                                list.stream()
                                        .map(pair -> pair.getKey().name())
                                        .collect(Collectors.joining(", "))));
                    } else {
                        String errors = list.stream()
                                .map(pair -> String.format("%s : %s", pair.getKey(),
                                        pair.getValue().isEmpty()
                                                ? "ok"
                                                : String.join("", pair.getValue())))
                                .collect(Collectors.joining("\n"));
                        promise.complete(String.format("Datamart '%s' check failed!\n%s",
                                context.getRequest().getQueryRequest().getDatamartMnemonic(), errors));
                    }
                })
                .onFailure(promise::fail));
    }

    private Future<Pair<SourceType, List<String>>> checkEntitiesBySourceType(List<Entity> entities,
                                                                             CheckContext context,
                                                                             SourceType sourceType) {
        return Future.future(promise -> CompositeFuture.join(entities.stream()
                .filter(entity -> entity.getDestination() == null || entity.getDestination().contains(sourceType))
                .filter(entity -> EntityType.TABLE.equals(entity.getEntityType()))
                .map(entity -> checkEntity(sourceType, context, entity))
                .collect(Collectors.toList()))
                .onSuccess(result -> {
                    List<Optional<String>> list = result.list();
                    List<String> errors = list.stream()
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .collect(Collectors.toList());
                    promise.complete(new Pair<>(sourceType, errors));
                })
                .onFailure(promise::fail));
    }

    private Future<Optional<String>> checkEntity(SourceType sourceType, CheckContext context, Entity entity) {
        return Future.future(promise -> dataSourcePluginService
                .checkTable(sourceType,
                        context.getMetrics(),
                        new CheckTableRequest(context.getRequest().getQueryRequest().getRequestId(),
                                context.getEnvName(),
                                context.getRequest().getQueryRequest().getDatamartMnemonic(),
                                entity))
                .onSuccess(result -> promise.complete(Optional.empty()))
                .onFailure(fail -> {
                    if (fail instanceof CheckException) {
                        promise.complete(Optional.of(
                                String.format("\n`%s` entity :%s", entity.getName(),
                                        fail.getMessage())));
                    } else {
                        promise.fail(fail);
                    }
                }));
    }

    @Override
    public CheckType getType() {
        return CheckType.DATABASE;
    }
}