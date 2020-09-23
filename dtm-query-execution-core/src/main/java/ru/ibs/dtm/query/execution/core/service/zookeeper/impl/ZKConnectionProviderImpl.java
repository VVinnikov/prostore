package ru.ibs.dtm.query.execution.core.service.zookeeper.impl;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
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
    private final AtomicBoolean synConnected = new AtomicBoolean();
    private final ZookeeperProperties properties;
    private ZooKeeper connection;

    public ZKConnectionProviderImpl(ZookeeperProperties properties) {
        this.properties = properties;
        initializeChroot();
    }

    private void initializeChroot() {
        try {
            connect(properties.getConnectionString())
                .create(properties.getChroot(), null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            log.info("Chroot node [{}] is created", properties.getChroot());
        } catch (KeeperException.NodeExistsException e) {
            log.debug("Chroot node [{}] is exists", properties.getChroot());
        } catch (Exception e) {
            String errMsg = String.format("Can't create chroot node [%s] for zk datasource", properties.getChroot());
            throw new RuntimeException(errMsg, e);
        } finally {
            close();
        }
    }

    @Override
    public ZooKeeper getOrConnect() {
        synchronized (synConnected) {
            return synConnected.get() && connection.getState().isConnected() ? connection : connect(getConnectionStringWithChroot());
        }
    }

    private String getConnectionStringWithChroot() {
        return properties.getConnectionString() + properties.getChroot();
    }

    @SneakyThrows
    private ZooKeeper connect(String connectionString) {
        if (connection != null) {
            connection.close();
        }
        val connectionLatch = new CountDownLatch(1);
        connection = new ZooKeeper(connectionString,
            properties.getSessionTimeoutMs(),
            we -> {
                log.debug("ZooKeeper connection: [{}]", we);
                if (we.getState() == SyncConnected) {
                    synConnected.set(true);
                    connectionLatch.countDown();
                } else {
                    synConnected.set(false);
                }
            });
        connectionLatch.await(properties.getConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
        if (!synConnected.get()) {
            val errMsg = String.format("Zookeeper connection timed out: [%d] ms", properties.getConnectionTimeoutMs());
            log.error(errMsg);
            throw new TimeoutException(errMsg);
        }
        return connection;
    }

    @Override
    @SneakyThrows
    public void close() {
        if (synConnected.get()) {
            connection.close();
        }
    }
}
