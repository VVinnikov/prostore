package ru.ibs.dtm.query.execution.core.dao.ddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.ext.sql.ResultSet;
import ru.ibs.dtm.common.model.ddl.ClassField;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;

import java.util.List;

public interface DdlServiceDao {

    void getMetadataByTableName(DdlRequestContext context, String table, Handler<AsyncResult<List<ClassField>>> resultHandler);

    void executeUpdate(String sql, Handler<AsyncResult<List<Void>>> resultHandler);

    void executeQuery(String sql, Handler<AsyncResult<ResultSet>> resultHandler);

    void dropTable(ClassTable classTable, Handler<AsyncResult<Void>> resultHandler);
}
