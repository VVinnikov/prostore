package ru.ibs.dtm.query.execution.core.dao.ddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.ext.sql.ResultSet;
import ru.ibs.dtm.common.model.ddl.Entity;
import ru.ibs.dtm.query.execution.model.metadata.ColumnMetadata;
import java.util.List;
import java.util.Map;

public interface DdlServiceDao {

    void executeUpdate(String sql, Handler<AsyncResult<List<Void>>> resultHandler);

    void executeQuery(String sql, List<ColumnMetadata> metadata, Handler<AsyncResult<List<Map<String, Object>>>> resultHandler);

    void dropTable(Entity entity, Handler<AsyncResult<Entity>> resultHandler);
}
