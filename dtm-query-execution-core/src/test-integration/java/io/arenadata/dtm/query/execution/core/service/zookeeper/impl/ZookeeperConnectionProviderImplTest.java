package io.arenadata.dtm.query.execution.core.service.zookeeper.impl;

import io.arenadata.dtm.query.execution.core.configuration.properties.ServiceDbZookeeperProperties;
import lombok.val;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZookeeperConnectionProviderImplTest {
    private static final int ZOOKEEPER_SIZE = 10;

    @Test
    public void test() throws InterruptedException {
        val connectionManager = new ZookeeperConnectionProviderImpl(getZookeeperProperties(), "TEST");
        List<ZooKeeper> zooKeeperList = new ArrayList<>();
        val connectionLatch = new CountDownLatch(ZOOKEEPER_SIZE);
        for (int i = 0; i < ZOOKEEPER_SIZE; i++) {
            new Thread(() -> {
                zooKeeperList.add(connectionManager.getOrConnect());
                connectionLatch.countDown();
            }).start();
        }
        connectionLatch.await(5, TimeUnit.SECONDS);
        assertEquals(ZOOKEEPER_SIZE, zooKeeperList.size());
        ZooKeeper expected = zooKeeperList.get(0);
        assertTrue(zooKeeperList.stream().allMatch(expected::equals));
    }

    private ZookeeperProperties getZookeeperProperties() {
        ZookeeperProperties properties = new ZookeeperProperties();
        properties.setSessionTimeoutMs(864_000);
        properties.setConnectionString("localhost");
        properties.setConnectionTimeoutMs(10_000);
        properties.setChroot("/testgration");
        return properties;
    }

}
