package io.arenadata.dtm.query.execution.core.service.zookeeper;

import org.apache.zookeeper.ZooKeeper;

public interface ZookeeperConnectionProvider {
    ZooKeeper getOrConnect();

    void close();
}
