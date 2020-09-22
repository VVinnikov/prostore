package ru.ibs.dtm.query.execution.core.service.zookeeper.impl;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.ZooKeeper;
import ru.ibs.dtm.query.execution.core.configuration.properties.ZookeeperProperties;
import ru.ibs.dtm.query.execution.core.service.zookeeper.ZKConnectionProvider;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.zookeeper.Watcher.Event.KeeperState.SyncConnected;

@Slf4j
public class ZKConnectionProviderImpl implements ZKConnectionProvider {
    private final AtomicBoolean connected = new AtomicBoolean();
    private final ZookeeperProperties properties;
    private ZooKeeper connection;

    public ZKConnectionProviderImpl(ZookeeperProperties properties) {
        this.properties = properties;
        initializeConnect();
    }

    private void initializeConnect() {
        connect();
    }

    @Override
    public ZooKeeper getOrConnect() {
        synchronized (connected) {
            return connected.get() ? connection : connect();
        }
    }

    @SneakyThrows
    private ZooKeeper connect() {
        if (connection != null) {
            connection.close();
        }
        val connectionLatch = new CountDownLatch(1);
        connection = new ZooKeeper(properties.getConnectionString(),
            properties.getSessionTimeoutMs(),
            we -> {
                log.debug("ZooKeeper connection: [{}]", we);
                if (we.getState() == SyncConnected) {
                    connected.set(true);
                    connectionLatch.countDown();
                } else {
                    connected.set(false);
                }
            });
        connectionLatch.await(properties.getConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
        if (!connected.get()) {
            val errMsg = String.format("Zookeeper connection timed out: [%d] ms", properties.getConnectionTimeoutMs());
            log.error(errMsg);
            throw new TimeoutException(errMsg);
        }
        return connection;
    }

    @Override
    @SneakyThrows
    public void close() {
        if (connected.get()) {
            connection.close();
        }
    }
}
