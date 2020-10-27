package io.arenadata.dtm.query.execution.core.factory.impl;

import io.arenadata.dtm.query.execution.core.utils.LocationUriParser;
import lombok.val;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LocationUriParserTest {

  public static final String LOCATION_PATH_WITHOUT_PORT = "kafka://localhost/topicX";
  public static final String LOCATION_PATH = "kafka://localhost:2181/topicX";
  public static final String EXPECTED_HOST = "localhost";
  public static final String EXPECTED_TOPIC = "topicX";
  public static final int EXPECTED_PORT = 2181;

  @Test
  void parseLocationPathWithZookeeperPort() {
    val topicUri = LocationUriParser.parseKafkaLocationPath(LOCATION_PATH);
    assertEquals(EXPECTED_HOST, topicUri.getHost());
    assertEquals(EXPECTED_TOPIC, topicUri.getTopic());
    assertEquals(EXPECTED_PORT, topicUri.getPort());
  }

  @Test
  void parseLocationPathWithoutZookeeperPort() {
    val topicUri = LocationUriParser.parseKafkaLocationPath(LOCATION_PATH_WITHOUT_PORT);
    assertEquals(EXPECTED_HOST, topicUri.getHost());
    assertEquals(EXPECTED_TOPIC, topicUri.getTopic());
    assertEquals(EXPECTED_PORT, topicUri.getPort());
  }

  @Test
  void badParseLocationPathWithoutZookeeperPort() {
    assertThrows(RuntimeException.class,
      () -> LocationUriParser.parseKafkaLocationPath("LOCATION_PATH_WITHOUT_PORT"),
      "Ошибка парсинга locationPath [LOCATION_PATH_WITHOUT_PORT]: null");
  }
}