package ru.ibs.dtm.query.execution.core.service.zookeeper;

import io.vertx.core.Future;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import ru.ibs.dtm.common.util.ThrowableConsumer;
import ru.ibs.dtm.common.util.ThrowableFunction;

import java.util.List;

public interface ZookeeperExecutor {
    Future<byte[]> getData(String path,
                           boolean watch,
                           Stat stat);

    Future<byte[]> getData(String path,
                           Watcher watcher,
                           Stat stat);

    Future<List<String>> getChildren(String path,
                                     Watcher watcher);

    Future<List<String>> getChildren(String path,
                                     boolean watch);

    Future<String> create(String path,
                          byte[] data,
                          List<ACL> acl,
                          CreateMode createMode);

    Future<Stat> setData(String path,
                         byte[] data,
                         int version);

    Future<List<OpResult>> multi(Iterable<Op> ops);

    Future<Void> delete(String path, int version);

    Future<Void> deleteRecursive(String pathRoot);

    <T> Future<T> execute(ThrowableFunction<ZooKeeper, T> function);

    Future<Void> executeVoid(ThrowableConsumer<ZooKeeper> callable);

}
