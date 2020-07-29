package ru.ibs.dtm.query.execution.core.dao.ddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.ext.sql.ResultSet;
import ru.ibs.dtm.common.model.ddl.ClassTable;

import java.util.List;

public interface DdlServiceDao {

    void executeUpdate(String sql, Handler<AsyncResult<List<Void>>> resultHandler);

    void executeQuery(String sql, Handler<AsyncResult<ResultSet>> resultHandler);

    void dropTable(ClassTable classTable, Handler<AsyncResult<ClassTable>> resultHandler);
}
