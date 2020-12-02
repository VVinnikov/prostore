package io.arenadata.dtm.query.execution.core.service.check;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.calcite.core.extension.check.SqlCheckData;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckException;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountParams;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Params;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckExecutor;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.apache.calcite.util.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

@Service("checkDataExecutor")
public class CheckDataExecutor implements CheckExecutor {
    private final DataSourcePluginService dataSourcePluginService;
    private final DeltaServiceDao deltaServiceDao;
    private final EntityDao entityDao;

    @Autowired
    public CheckDataExecutor(DataSourcePluginService dataSourcePluginService,
                             DeltaServiceDao deltaServiceDao, EntityDao entityDao) {
        this.dataSourcePluginService = dataSourcePluginService;
        this.deltaServiceDao = deltaServiceDao;
        this.entityDao = entityDao;
    }

    @Override
    public Future<String> execute(CheckContext context) {
        SqlCheckData sqlCheckData = (SqlCheckData) context.getSqlCheckCall();
        return entityDao.getEntity(context.getRequest().getQueryRequest().getDatamartMnemonic(), sqlCheckData.getTable())
                .compose(entity -> EntityType.TABLE.equals(entity.getEntityType())
                        ? Future.succeededFuture(entity)
                        : Future.failedFuture(new IllegalArgumentException(
                        String.format("Table `%s` not exist", sqlCheckData.getTable()))))
                .compose(entity -> check(context, entity, sqlCheckData));
    }

    @Override
    public CheckType getType() {
        return CheckType.DATA;
    }

    private Future<String> check(CheckContext context,
                                 Entity entity,
                                 SqlCheckData sqlCheckData) {
        BiFunction<SourceType, Long, Future<Long>> checkFunc = Optional.ofNullable(sqlCheckData.getColumns())
                .map(columns -> getCheckHashFunc(context, entity, columns))
                .orElse((type, sysCn) -> dataSourcePluginService.checkDataByCount(
                        new CheckDataByCountParams(type, context.getMetrics(), entity, sysCn,
                                context.getRequest().getQueryRequest().getEnvName())));
        return Future.future(promise -> check(sqlCheckData.getDeltaNum(), entity.getSchema(), checkFunc)
                .onSuccess(result -> promise.complete(""))
                .onFailure(exception -> {
                    if (exception instanceof CheckException) {
                        promise.complete(String.format("Table '%s.%s' checksum mismatch!\n%s",
                                entity.getSchema(), entity.getName(), exception.getMessage()));
                    } else {
                        promise.fail(exception);
                    }
                }));
    }

    private Future<Void> check(Long deltaNum,
                               String datamart,
                               BiFunction<SourceType, Long, Future<Long>> checkFunc) {
        return deltaServiceDao.getDeltaOk(datamart)
                .compose(deltaOk -> deltaServiceDao.getDeltaByNum(datamart, deltaNum)
                        .compose(delta -> Future.succeededFuture(getCheckRange(delta.getCnFrom(), deltaOk.getCnTo()))))
                .compose(checkRange -> checkByRange(checkRange, checkFunc));
    }

    private Future<Void> checkByRange(List<Long> checkRange,
                                      BiFunction<SourceType, Long, Future<Long>> checkFunc) {

        return Future.future(promise -> CompositeFuture.join(checkRange.stream()
                .map(sysCn -> checkBySysCn(sysCn, checkFunc))
                .collect(Collectors.toList()))
                .onSuccess(result -> promise.complete())
                .onFailure(promise::fail));
    }

    private Future<Void> checkBySysCn(Long sysCn, BiFunction<SourceType, Long, Future<Long>> checkFunc) {
        return Future.future(promise -> CompositeFuture.all(
                dataSourcePluginService.getSourceTypes().stream()
                        .map(sourceType -> checkFunc.apply(sourceType, sysCn)
                                .compose(val -> Future.succeededFuture(new Pair<>(sourceType, val))))
                        .collect(Collectors.toList()))
                .onSuccess(result -> {
                    List<Pair<SourceType, Long>> resultList = result.list();
                    if (resultList.stream().map(Pair::getValue).distinct().count() == 1) {
                        promise.complete();
                    } else {
                        promise.fail(new CheckException(resultList.stream()
                                .map(pair -> String.format("%s: %s", pair.getKey(), pair.getValue().toString()))
                                .collect(Collectors.joining("\n"))));
                    }
                })
                .onFailure(promise::fail));
    }

    private BiFunction<SourceType, Long, Future<Long>> getCheckHashFunc(CheckContext context,
                                                                        Entity entity,
                                                                        Set<String> columns) {
        Set<String> entityFieldNames = entity.getFields().stream()
                .map(EntityField::getName)
                .collect(Collectors.toSet());
        Set<String> notExistColumns = columns.stream()
                .filter(column -> !entityFieldNames.contains(column))
                .collect(Collectors.toSet());
        if (!notExistColumns.isEmpty()) {
            throw new IllegalArgumentException(String.format("Columns: `%s` don't exist.",
                    String.join(", ", notExistColumns)));
        } else {
            return (type, sysCn) -> dataSourcePluginService.checkDataByHashInt32(
                    new CheckDataByHashInt32Params(type, context.getMetrics(), entity, sysCn, columns,
                            context.getRequest().getQueryRequest().getEnvName()));
        }
    }

    private List<Long> getCheckRange(Long cnFrom, Long cnTo) {
        List<Long> result = new ArrayList<>();
        for (Long i = cnTo; i >= cnFrom; i--) {
            result.add(i);
        }
        return result;
    }
}
