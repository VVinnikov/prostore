package io.arenadata.dtm.query.execution.plugin.adb.ddl.service;

import io.arenadata.dtm.query.execution.plugin.adb.ddl.factory.DdlSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlService;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class DropTableExecutor implements DdlExecutor<Void> {

    private final DatabaseExecutor adbQueryExecutor;
    private final DdlSqlFactory sqlFactory;

    @Autowired
    public DropTableExecutor(@Qualifier("adbQueryExecutor") DatabaseExecutor adbQueryExecutor, DdlSqlFactory sqlFactory) {
        this.adbQueryExecutor = adbQueryExecutor;
        this.sqlFactory = sqlFactory;
    }

    @Override
    public Future<Void> execute(DdlRequest request) {
        return Future.future(promise -> {
            String dropSql = sqlFactory.createDropTableScript(request.getEntity().getNameWithSchema());
            adbQueryExecutor.executeUpdate(dropSql)
                    .onComplete(promise);
        });
    }

    @Override
    public SqlKind getSqlKind() {
        return SqlKind.DROP_TABLE;
    }

    @Override
    @Autowired
    public void register(@Qualifier("adbDdlService") DdlService<Void> service) {
        service.addExecutor(this);
    }
}
