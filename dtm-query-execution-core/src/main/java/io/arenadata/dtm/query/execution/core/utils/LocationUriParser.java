package io.arenadata.dtm.query.execution.core.utils;

import io.arenadata.dtm.kafka.core.configuration.kafka.KafkaZookeeperProperties;
import io.arenadata.dtm.query.execution.core.exception.DtmException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class LocationUriParser {

    private static final int DEFAULT_ZOOKEEPER_PORT = 2181;
    private static final String HOST_DELIMITER = ":";
    private static final String HOSTS_DELIMITER = ",";
    private KafkaZookeeperProperties kafkaZookeeperProperties;

    @Autowired
    public LocationUriParser(KafkaZookeeperProperties kafkaZookeeperProperties) {
        this.kafkaZookeeperProperties = kafkaZookeeperProperties;
    }

    public KafkaTopicUri parseKafkaLocationPath(String locationPath) {
        try {
            URI uri = URI.create(locationPath);
            val lastSlashIdx = uri.getPath().lastIndexOf("/");
            val topic = uri.getPath().substring(lastSlashIdx + 1);
            val chroot = uri.getPath().substring(0, lastSlashIdx);
            if (uri.getAuthority().equals("$kafka")) {
                return new KafkaTopicUri(Collections.singletonList(kafkaZookeeperProperties.getConnectionString() + ":" + DEFAULT_ZOOKEEPER_PORT), kafkaZookeeperProperties.getChroot(), topic);
            } else {
                val hosts = uri.getAuthority().split(HOSTS_DELIMITER);
                List<String> hostArray = Arrays.stream(hosts).map(hostPort -> {
                    val hostPortArray = hostPort.split(HOST_DELIMITER);
                    val host = hostPortArray[0];
                    val port = hostPortArray.length > 1 ? Integer.parseInt(hostPortArray[1]) : DEFAULT_ZOOKEEPER_PORT;
                    return host + HOST_DELIMITER + port;
                }).collect(Collectors.toList());
                return new KafkaTopicUri(hostArray, chroot, topic);
            }
        } catch (Exception e) {
            String errMsg = String.format("LocationPath parsing error [%s]: %s", locationPath, e.getMessage());
            log.error(errMsg, e);
            throw new DtmException(errMsg, e);
        }
    }

    @Data
    @AllArgsConstructor
    public final static class KafkaTopicUri {
        private List<String> hosts;
        private String chroot;
        private String topic;

        public String getAddress() {
            return String.join(HOSTS_DELIMITER, this.hosts) + this.chroot;
        }
    }
}
