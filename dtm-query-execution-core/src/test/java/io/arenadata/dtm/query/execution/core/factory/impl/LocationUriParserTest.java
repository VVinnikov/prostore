package io.arenadata.dtm.query.execution.core.factory.impl;

import io.arenadata.dtm.kafka.core.configuration.kafka.KafkaZookeeperProperties;
import io.arenadata.dtm.query.execution.core.utils.LocationUriParser;
import lombok.val;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LocationUriParserTest {

  private static final String LOCATION_PATH_PLACEHOLDER = "kafka://$kafka/topicX";
  private static final String LOCATION_PATH_WITHOUT_PORT = "kafka://localhost/topicX";
  private static final String LOCATION_PATH = "kafka://localhost:2181/topicX";
  private static final String LOCATION_PATH_WITH_MULTIPLE_HOSTS = "kafka://localhost1:2181,localhost2:2181/chroot/topicX";
  private static final String EXPECTED_ADDRESS_MULTIPLE_HOSTS = "localhost1:2181,localhost2:2181/chroot";
  private static final String EXPECTED_ADDRESS_PLACEHOLDER = "localhost:2181/chroot";
  private static final String EXPECTED_ADDRESS = "localhost:2181";
  private static final String EXPECTED_CHROOT = "chroot";
  private static final String EXPECTED_TOPIC = "topicX";
  private static final String ZOOKEEPER_PROPERTIES_CONNECTION_STRING = "localhost";

  private KafkaZookeeperProperties zookeeperProperties = mock(KafkaZookeeperProperties.class);
  private LocationUriParser locationUriParser = new LocationUriParser(zookeeperProperties);

  @Test
  void parseLocationPathWithZookeeperPort() {
    val topicUri = locationUriParser.parseKafkaLocationPath(LOCATION_PATH);
    assertEquals(EXPECTED_ADDRESS, topicUri.getAddress());
    assertEquals(EXPECTED_TOPIC, topicUri.getTopic());
  }

  @Test
  void parseLocationPathWithoutZookeeperPort() {
    val topicUri = locationUriParser.parseKafkaLocationPath(LOCATION_PATH_WITHOUT_PORT);
    assertEquals(EXPECTED_ADDRESS, topicUri.getAddress());
    assertEquals(EXPECTED_TOPIC, topicUri.getTopic());
  }

  @Test
  void parseLocationPathWithMultipleHosts() {
    val topicUri = locationUriParser.parseKafkaLocationPath(LOCATION_PATH_WITH_MULTIPLE_HOSTS);
    assertEquals(EXPECTED_ADDRESS_MULTIPLE_HOSTS, topicUri.getAddress());
    assertEquals(EXPECTED_TOPIC, topicUri.getTopic());
  }

  @Test
  void parseLocationPathWithPlaceholder() {
    when(zookeeperProperties.getConnectionString()).thenReturn(ZOOKEEPER_PROPERTIES_CONNECTION_STRING);
    when(zookeeperProperties.getChroot()).thenReturn("/" + EXPECTED_CHROOT);
    val topicUri = locationUriParser.parseKafkaLocationPath(LOCATION_PATH_PLACEHOLDER);
    assertEquals(EXPECTED_ADDRESS_PLACEHOLDER, topicUri.getAddress());
    assertEquals(EXPECTED_TOPIC, topicUri.getTopic());
  }

  @Test
  void parseLocationPathWithPlaceholderWithoutChroot() {
    when(zookeeperProperties.getConnectionString()).thenReturn(ZOOKEEPER_PROPERTIES_CONNECTION_STRING);
    when(zookeeperProperties.getChroot()).thenReturn("");
    val topicUri = locationUriParser.parseKafkaLocationPath(LOCATION_PATH_PLACEHOLDER);
    assertEquals(EXPECTED_ADDRESS, topicUri.getAddress());
    assertEquals(EXPECTED_TOPIC, topicUri.getTopic());
  }

  @Test
  void badParseLocationPathWithoutZookeeperPort() {
    assertThrows(RuntimeException.class,
      () -> locationUriParser.parseKafkaLocationPath("LOCATION_PATH_WITHOUT_PORT"),
      "Parsing error locationPath [LOCATION_PATH_WITHOUT_PORT]: null");
  }
}
