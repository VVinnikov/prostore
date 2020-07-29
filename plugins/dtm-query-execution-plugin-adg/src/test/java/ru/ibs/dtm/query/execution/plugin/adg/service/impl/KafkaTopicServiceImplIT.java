package ru.ibs.dtm.query.execution.plugin.adg.service.impl;

import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import ru.ibs.dtm.kafka.core.service.kafka.KafkaTopicService;
import ru.ibs.dtm.query.execution.plugin.adg.service.DtmTestConfiguration;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

@SpringBootTest(classes = DtmTestConfiguration.class)
@ExtendWith(VertxExtension.class)
class KafkaTopicServiceImplIT {

  @Autowired
  KafkaTopicService kafkaTopicService;

  @Test
  void createOrReplaceTopic(VertxTestContext testContext) throws Throwable {
    kafkaTopicService.createOrReplace(Collections.singletonList("test.creator1"), ar -> {
      if (ar.succeeded()) {
        testContext.completeNow();
      } else {
        testContext.failNow(ar.cause());
      }
    });
    testContext.awaitCompletion(5, TimeUnit.SECONDS);
  }
}
