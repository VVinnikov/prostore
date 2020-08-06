package ru.ibs.dtm.query.execution.core.utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;

@Slf4j
public class LocationUriParser {

    public static final int DEFAULT_ZOOKEEPER_PORT = 2181;

    public static KafkaTopicUri parseKafkaLocationPath(String locationPath) {
        try {
            URI uri = URI.create(locationPath);
            String topic = uri.getPath().substring(1);
            String[] authorityArray = uri.getAuthority().split(":");
            String host = authorityArray[0];
            int port = authorityArray.length > 1 ? Integer.parseInt(authorityArray[1]) : DEFAULT_ZOOKEEPER_PORT;
            return new KafkaTopicUri(host, topic, port);
        } catch (Exception e) {
            String errMsg = String.format("LocationPath parsing error [%s]: %s", locationPath, e.getMessage());
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Data
    @AllArgsConstructor
    public final static class KafkaTopicUri {
        private String host;
        private String topic;
        private int port;
    }
}
