package io.arenadata.dtm.query.execution.plugin.adb.service.impl.ddl;

import io.arenadata.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.DdlService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class DropTableExecutor implements DdlExecutor<Void> {

    private final AdbQueryExecutor adbQueryExecutor;
    private final MetadataSqlFactory sqlFactory;

    @Autowired
    public DropTableExecutor(AdbQueryExecutor adbQueryExecutor, MetadataSqlFactory sqlFactory) {
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
