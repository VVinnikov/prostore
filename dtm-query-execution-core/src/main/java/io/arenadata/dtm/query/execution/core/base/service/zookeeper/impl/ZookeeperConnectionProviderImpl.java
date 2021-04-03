package io.arenadata.dtm.query.execution.core.base.service.zookeeper.impl;

import io.arenadata.dtm.query.execution.core.base.configuration.properties.ServiceDbZookeeperProperties;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.query.execution.core.base.service.zookeeper.ZookeeperConnectionProvider;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.zookeeper.Watcher.Event.KeeperState.SyncConnected;

@Slf4j
public class ZookeeperConnectionProviderImpl implements ZookeeperConnectionProvider {
    private final ServiceDbZookeeperProperties properties;
    private ZooKeeper connection;
    private boolean synConnected;

    public ZookeeperConnectionProviderImpl(ServiceDbZookeeperProperties properties, String envName) {
        this.properties = properties;
        initializeChroot(envName);
    }

    private void initializeChroot(String envName) {
        try {
            connect(properties.getConnectionString())
                .create(properties.getChroot(), null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            log.info("Chroot node [{}] is created", properties.getChroot());
            initializeEnv(envName);
        } catch (KeeperException.NodeExistsException e) {
            log.debug("Chroot node [{}] is exists", properties.getChroot());
            initializeEnv(envName);
        } catch (Exception e) {
            String errMsg = String.format("Can't create chroot node [%s] for zk datasource", properties.getChroot());
            throw new DtmException(errMsg, e);
        } finally {
            close();
        }
    }

    private void initializeEnv(String envName) {
        String envNodePath = properties.getChroot() + "/" + envName;
        try {
            connect(properties.getConnectionString())
                .create(envNodePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            log.info("Env node [{}] is created", envNodePath);
        } catch (KeeperException.NodeExistsException e) {
            log.debug("Env node [{}] is exists", envNodePath);
        } catch (Exception e) {
            String errMsg = String.format("Can't create env node [%s] for zk datasource", envNodePath);
            throw new DtmException(errMsg, e);
        }
    }

    @Override
    public ZooKeeper getOrConnect() {
        return synConnected && connection.getState().isConnected() ? connection : connect(getConnectionStringWithChroot());
    }

    private String getConnectionStringWithChroot() {
        return properties.getConnectionString() + properties.getChroot();
    }

    @SneakyThrows
    private synchronized ZooKeeper connect(String connectionString) {
        if (connection != null) {
            if (connection.getState().isConnected()) {
                return connection;
            } else {
                connection.close();
            }
        }
        val connectionLatch = new CountDownLatch(1);
        connection = new ZooKeeper(connectionString,
            properties.getSessionTimeoutMs(),
            we -> {
                log.debug("ZooKeeper connection: [{}]", we);
                if (we.getState() == SyncConnected) {
                    synConnected = true;
                    connectionLatch.countDown();
                } else {
                    synConnected = false;
                }
            });
        connectionLatch.await(properties.getConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
        if (!synConnected) {
            val errMsg = String.format("Zookeeper connection timed out: [%d] ms",
                    properties.getConnectionTimeoutMs());
            throw new DtmException(errMsg);
        }
        return connection;
    }

    @Override
    @SneakyThrows
    public void close() {
        if (synConnected) {
            connection.close();
            synConnected = false;
        }
    }
}
