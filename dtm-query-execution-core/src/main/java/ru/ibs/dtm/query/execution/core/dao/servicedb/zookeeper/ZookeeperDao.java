package ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper;

public interface ZookeeperDao<T> {
    String getTargetPath(T target);
}
