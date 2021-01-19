package io.arenadata.dtm.query.execution.plugin.adb.service.impl.ddl;

import io.arenadata.dtm.query.execution.plugin.adb.AdbDtmDataSourcePlugin;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.exception.DdlDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlService;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service("adbDdlService")
public class AdbDdlService implements DdlService<Void> {

    private final Map<SqlKind, DdlExecutor<Void>> ddlExecutors = new HashMap<>();

    @Override
    @CacheEvict(value = AdbDtmDataSourcePlugin.ADB_DATAMART_CACHE, key = "#request.getDatamartName()")
    public Future<Void> execute(DdlRequestContext request) {
        return Future.future(promise -> {
            SqlNode query = request.getQuery();
            if (query == null) {
                promise.fail(new DdlDatasourceException("Ddl query is null!"));
                return;
            }
            if (ddlExecutors.containsKey(query.getKind())) {
                ddlExecutors.get(query.getKind())
                        .execute(request, query.getKind().lowerName)
                        .onComplete(promise);
            } else {
                promise.fail(new DdlDatasourceException(String.format("Unknown DDL: %s",
                        query)));
            }
        });
    }

    @Override
    public void addExecutor(DdlExecutor<Void> executor) {
        ddlExecutors.put(executor.getSqlKind(), executor);
    }
}
