package io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper;

import io.arenadata.dtm.query.execution.core.dto.metadata.DatamartInfo;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.util.List;

public interface DatamartDao extends ZookeeperDao<String> {
    Future<Void> createDatamart(String name);

    void getDatamartMeta(Handler<AsyncResult<List<DatamartInfo>>> resultHandler);

    Future<List<String>> getDatamarts();

    Future<?> getDatamart(String name);

    Future<Boolean> existsDatamart(String name);

    Future<Void> deleteDatamart(String name);
}
