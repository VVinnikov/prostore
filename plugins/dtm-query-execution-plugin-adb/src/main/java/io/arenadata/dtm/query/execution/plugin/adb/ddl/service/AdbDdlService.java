package io.arenadata.dtm.query.execution.plugin.adb.ddl.service;

import io.arenadata.dtm.query.execution.plugin.adb.base.service.AdbDtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.exception.DdlDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlService;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlKind;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service("adbDdlService")
public class AdbDdlService implements DdlService<Void> {

    private final Map<SqlKind, DdlExecutor<Void>> ddlExecutors = new HashMap<>();

    @Override
    @CacheEvict(value = AdbDtmDataSourcePlugin.ADB_DATAMART_CACHE, key = "#request.getDatamartMnemonic()")
    public Future<Void> execute(DdlRequest request) {
        SqlKind sqlKind = request.getSqlKind();
        if (ddlExecutors.containsKey(sqlKind)) {
            return ddlExecutors.get(sqlKind)
                    .execute(request);
        } else {
            return Future.failedFuture(new DdlDatasourceException(String.format("Unknown DDL: %s", sqlKind)));
        }
    }

    @Override
    public void addExecutor(DdlExecutor<Void> executor) {
        ddlExecutors.put(executor.getSqlKind(), executor);
    }
}
