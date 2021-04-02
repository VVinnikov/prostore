package io.arenadata.dtm.query.execution.core.base.repository.zookeeper;

public interface ZookeeperDao<T> {
    String getTargetPath(T target);
}
