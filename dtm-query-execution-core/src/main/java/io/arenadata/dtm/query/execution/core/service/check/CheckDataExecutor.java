package io.arenadata.dtm.query.execution.core.service.check;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.extension.check.CheckType;
import io.arenadata.dtm.query.calcite.core.extension.check.SqlCheckData;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.service.DataSourcePluginService;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByCountParams;
import io.arenadata.dtm.query.execution.plugin.api.dto.CheckDataByHashInt32Params;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckExecutor;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

@Service("checkDataExecutor")
public class CheckDataExecutor implements CheckExecutor {
    private final DataSourcePluginService dataSourcePluginService;
    private final EntityDao entityDao;

    @Autowired
    public CheckDataExecutor(DataSourcePluginService dataSourcePluginService,
                             EntityDao entityDao) {
        this.dataSourcePluginService = dataSourcePluginService;
        this.entityDao = entityDao;
    }

    @Override
    public Future<String> execute(CheckContext context) {
        SqlCheckData sqlCheckData = (SqlCheckData) context.getSqlCheckCall();
        return entityDao.getEntity(context.getRequest().getQueryRequest().getDatamartMnemonic(), sqlCheckData.getTable())
                .compose(entity -> check(context, entity, sqlCheckData.getDeltaNum(), sqlCheckData.getColumns()));
    }

    @Override
    public CheckType getType() {
        return CheckType.DATA;
    }

    private Future<String> check(CheckContext context,
                                 Entity entity,
                                 Long sysCn,
                                 Set<String> columns) {
        return CompositeFuture.join(
                dataSourcePluginService.getSourceTypes().stream()
                        .map(sourceType -> checkByType(sourceType, context, entity, sysCn, columns))
                        .collect(Collectors.toList()))
                .map(result -> String.join("\n", result.list()));
    }

    private Future<String> checkByType(SourceType sourceType,
                                       CheckContext context,
                                       Entity entity,
                                       Long sysCn,
                                       Set<String> columns) {
        CheckDataByCountParams checkDataByCountParams = new CheckDataByCountParams(sourceType,
                context.getMetrics(), entity, sysCn);
        CheckDataByHashInt32Params checkDataByHashInt32Params = new CheckDataByHashInt32Params(sourceType,
                context.getMetrics(), entity, sysCn, columns);
        return Future.future(promise -> CompositeFuture.join(
                Arrays.asList(dataSourcePluginService.checkDataByCount(checkDataByCountParams),
                        dataSourcePluginService.checkDataByHashInt32(checkDataByHashInt32Params)))
                .onSuccess(result -> promise.complete(String.format("%s:\n%s",
                        sourceType, String.join("\n", result.list()))))
                .onFailure(promise::fail)
        );
    }
}
