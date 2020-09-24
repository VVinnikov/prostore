package ru.ibs.dtm.query.execution.core.service.zookeeper.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.RequiredArgsConstructor;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import ru.ibs.dtm.common.util.ThrowableConsumer;
import ru.ibs.dtm.common.util.ThrowableFunction;
import ru.ibs.dtm.query.execution.core.service.zookeeper.ZKConnectionProvider;
import ru.ibs.dtm.query.execution.core.service.zookeeper.ZookeeperExecutor;

import java.util.List;

@RequiredArgsConstructor
public class ZookeeperExecutorImpl implements ZookeeperExecutor {
    private final ZKConnectionProvider connectionManager;
    private final Vertx vertx;

    @Override
    public Future<byte[]> getData(String path, boolean watch, Stat stat) {
        return execute(zk -> zk.getData(path, watch, stat));
    }

    @Override
    public Future<byte[]> getData(String path, Watcher watcher, Stat stat) {
        return execute(zk -> zk.getData(path, watcher, stat));
    }

    @Override
    public Future<List<String>> getChildren(String path, Watcher watcher) {
        return execute(zk -> zk.getChildren(path, watcher));
    }

    @Override
    public Future<List<String>> getChildren(String path, boolean watch) {
        return execute(zk -> zk.getChildren(path, watch));
    }

    @Override
    public Future<String> create(final String path,
                                 byte[] data,
                                 List<ACL> acl,
                                 CreateMode createMode) {
        return execute(zk -> zk.create(path, data, acl, createMode));
    }

    @Override
    public Future<Stat> setData(String path, byte[] data, int version) {
        return execute(zk -> zk.setData(path, data, version));
    }

    @Override
    public Future<List<OpResult>> multi(Iterable<Op> ops) {
        return execute(zk -> zk.multi(ops));
    }

    @Override
    public Future<Void> delete(String path, int version) {
        return executeVoid(zk -> zk.delete(path, version));
    }

    @Override
    public Future<Void> deleteRecursive(String pathRoot) {
        return executeVoid(zk -> ZKUtil.deleteRecursive(zk, pathRoot));
    }

    @Override
    public <T> Future<T> execute(ThrowableFunction<ZooKeeper, T> function) {
        return Future.future(promise -> vertx.executeBlocking(blockingPromise -> {
            try {
                blockingPromise.complete(function.apply(connectionManager.getOrConnect()));
            } catch (Exception e) {
                blockingPromise.fail(e);
            }
        }, promise));
    }

    @Override
    public Future<Void> executeVoid(ThrowableConsumer<ZooKeeper> consumer) {
        return Future.future(promise -> vertx.executeBlocking(blockingPromise -> {
            try {
                consumer.accept(connectionManager.getOrConnect());
                blockingPromise.complete();
            } catch (Exception e) {
                blockingPromise.fail(e);
            }
        }, promise));
    }
}
