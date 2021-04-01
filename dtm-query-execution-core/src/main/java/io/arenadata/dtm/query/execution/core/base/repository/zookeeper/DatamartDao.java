package io.arenadata.dtm.query.execution.core.base.repository.zookeeper;

import io.arenadata.dtm.query.execution.core.metadata.dto.DatamartInfo;
import io.vertx.core.Future;

import java.util.List;

public interface DatamartDao extends ZookeeperDao<String> {
    Future<Void> createDatamart(String name);

    Future<List<DatamartInfo>> getDatamartMeta();

    Future<List<String>> getDatamarts();

    Future<byte[]> getDatamart(String name);

    Future<Boolean> existsDatamart(String name);

    Future<Void> deleteDatamart(String name);
}
