package io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper;

public interface ZookeeperDao<T> {
    String getTargetPath(T target);
}
