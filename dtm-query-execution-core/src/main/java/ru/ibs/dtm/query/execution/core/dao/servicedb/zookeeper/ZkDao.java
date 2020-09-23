package ru.ibs.dtm.query.execution.core.dao.servicedb.zookeeper;

public interface ZkDao<T> {
    String getTargetPath(T target);
}
