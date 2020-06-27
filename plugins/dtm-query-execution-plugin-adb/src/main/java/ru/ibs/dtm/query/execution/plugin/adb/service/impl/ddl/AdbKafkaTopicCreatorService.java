package ru.ibs.dtm.query.execution.plugin.adb.service.impl.ddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.admin.NewTopic;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.execution.plugin.adb.service.KafkaTopicCreatorService;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
public class AdbKafkaTopicCreatorService implements KafkaTopicCreatorService {

  private KafkaAdminClient adminClient;

  @Autowired
  public AdbKafkaTopicCreatorService(@Qualifier("adbKafkaAdminClient") KafkaAdminClient adminClient) {
    this.adminClient = adminClient;
  }

  @Override
  public void create(List<String> topics, Handler<AsyncResult<Void>> handler) {
    adminClient.createTopics(topics.stream().map(it -> new NewTopic(it, 1, (short) 1)).collect(Collectors.toList()), ar -> {
      if (ar.succeeded()) {
        log.debug("Топики [{}] успешно созданы", String.join(",", topics));
        handler.handle(Future.succeededFuture());
      } else {
        log.error("Ошибка создания топиков [{}]", String.join(",", topics), ar.cause());
        if (ar.cause() instanceof TopicExistsException) {
          handler.handle(Future.succeededFuture());
          return;
        }
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  @Override
  public void delete(List<String> topics, Handler<AsyncResult<Void>> handler) {
    adminClient.deleteTopics(topics, ar -> {
      if (ar.succeeded()) {
        log.debug("Топики [{}] успешно удалены", String.join(",", topics));
        handler.handle(Future.succeededFuture());
      } else {
        log.error("Ошибка удаления топиков [{}]", String.join(",", topics), ar.cause());
        if (ar.cause() instanceof UnknownTopicOrPartitionException) {
          handler.handle(Future.succeededFuture());
          return;
        }
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  @Override
  public void createOrReplace(List<String> topics, Handler<AsyncResult<Void>> handler) {
    delete(topics, ignored -> create(topics, ar -> {
      if (ar.succeeded()) {
        handler.handle(Future.succeededFuture());
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    }));
  }
}
