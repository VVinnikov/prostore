package io.arenadata.dtm.query.execution.core.factory.impl;

import io.arenadata.dtm.query.execution.core.utils.LocationUriParser;
import lombok.val;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LocationUriParserTest {

  public static final String LOCATION_PATH_WITHOUT_PORT = "kafka://localhost/topicX";
  public static final String LOCATION_PATH = "kafka://localhost:2181/topicX";
  public static final String LOCATION_PATH_WITH_MULTIPLE_HOSTS = "kafka://localhost1:2181,localhost2:2181/chroot/topicX";
  public static final String EXPECTED_ADDRESS_MULTIPLE_HOSTS = "localhost1:2181,localhost2:2181/chroot";
  public static final String EXPECTED_ADDRESS = "localhost:2181";
  public static final String EXPECTED_TOPIC = "topicX";

  @Test
  void parseLocationPathWithZookeeperPort() {
    val topicUri = LocationUriParser.parseKafkaLocationPath(LOCATION_PATH);
    assertEquals(EXPECTED_ADDRESS, topicUri.getAddress());
    assertEquals(EXPECTED_TOPIC, topicUri.getTopic());
  }

  @Test
  void parseLocationPathWithoutZookeeperPort() {
    val topicUri = LocationUriParser.parseKafkaLocationPath(LOCATION_PATH_WITHOUT_PORT);
    assertEquals(EXPECTED_ADDRESS, topicUri.getAddress());
    assertEquals(EXPECTED_TOPIC, topicUri.getTopic());
  }

  @Test
  void parseLocationPathWithMultipleHosts() {
    val topicUri = LocationUriParser.parseKafkaLocationPath(LOCATION_PATH_WITH_MULTIPLE_HOSTS);
    assertEquals(EXPECTED_ADDRESS_MULTIPLE_HOSTS, topicUri.getAddress());
    assertEquals(EXPECTED_TOPIC, topicUri.getTopic());
  }

  @Test
  void badParseLocationPathWithoutZookeeperPort() {
    assertThrows(RuntimeException.class,
      () -> LocationUriParser.parseKafkaLocationPath("LOCATION_PATH_WITHOUT_PORT"),
      "Parsing error locationPath [LOCATION_PATH_WITHOUT_PORT]: null");
  }
}
