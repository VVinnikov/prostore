package ru.ibs.dtm.query.execution.plugin.adg.service.impl;

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
import ru.ibs.dtm.query.execution.plugin.adg.service.KafkaTopicService;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
public class KafkaTopicServiceImpl implements KafkaTopicService {

  private KafkaAdminClient adminClient;

  @Autowired
  public KafkaTopicServiceImpl(@Qualifier("coreKafkaAdminClient") KafkaAdminClient adminClient) {
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
    delete(topics, ignored -> {
      //Получаем список топиков, так как в Кафке может быть запрещено удление топиков прямым образом
      topics(ar -> {
        if (ar.succeeded()) {
          List<String> topicsForCreate = topics.stream().filter(element ->
            !ar.result().stream().anyMatch(match -> match.equalsIgnoreCase(element))).collect(Collectors.toList());
          log.debug("Создание топиков [{}]", String.join(",", topicsForCreate));
          create(topicsForCreate, ar1 -> {
            if (ar1.succeeded()) {
              handler.handle(Future.succeededFuture());
            } else {
              handler.handle(Future.failedFuture(ar1.cause()));
            }
          });
        } else {
          handler.handle(Future.failedFuture(ar.cause()));
        }
      });
    });
  }

  @Override
  public void topics(Handler<AsyncResult<Set<String>>> handler) {
    adminClient.listTopics(ar -> {
      if (ar.succeeded()) {
        handler.handle(Future.succeededFuture(ar.result()));
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }
}
