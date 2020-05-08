package ru.ibs.dtm.query.execution.plugin.adb.service.impl.ddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.query.execution.plugin.adb.configuration.kafka.KafkaAdminProperty;
import ru.ibs.dtm.query.execution.plugin.adb.configuration.properties.KafkaProperties;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MetadataFactory;
import ru.ibs.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.adb.service.KafkaTopicCreatorService;
import ru.ibs.dtm.query.execution.plugin.api.dto.DdlRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.DdlService;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;

@Service("adbDdlService")
public class AdbDdlService implements DdlService {

  private static final Logger LOGGER = LoggerFactory.getLogger(AdbDdlService.class);
  static final Predicate<String> IS_CREATE_TABLE = predicate("\\s*CREATE\\s+TABLE.*");
  static final Predicate<String> IS_DROP_TABLE = predicate("\\s*DROP\\s+TABLE.*");
  static final Predicate<String> IS_CREATE_SCHEMA = predicate("\\s*CREATE\\s+SCHEMA.*");
  static final Predicate<String> IS_DROP_SCHEMA = predicate("\\s*DROP\\s+SCHEMA.*");

  private final MetadataFactory metadataFactory;
  private final DatabaseExecutor adbDatabaseExecutor;
  private final KafkaTopicCreatorService kafkaTopicService;
  private final KafkaProperties kafkaProperties;
  private final Vertx vertx;

  @Autowired
  public AdbDdlService(MetadataFactory metadataFactory,
                       DatabaseExecutor adbDatabaseExecutor, KafkaTopicCreatorService kafkaTopicService,
                       KafkaProperties kafkaProperties,
                       @Qualifier("adbVertx") Vertx vertx
  ) {
    this.metadataFactory = metadataFactory;
    this.adbDatabaseExecutor = adbDatabaseExecutor;
    this.kafkaTopicService = kafkaTopicService;
    this.kafkaProperties = kafkaProperties;
    this.vertx = vertx;
  }

  @Override
  public void execute(DdlRequest request, Handler<AsyncResult<Void>> handler) {
    final String sql = request.getQueryRequest().getSql();
    if (IS_CREATE_TABLE.test(sql)) {
      createTable(request, handler);
      return;
    }
    if (IS_DROP_TABLE.test(sql)) {
      dropTable(request, handler);
      return;
    }
    if (IS_CREATE_SCHEMA.test(sql)) {
      applySql(sql, handler);
      return;
    }
    if (IS_DROP_SCHEMA.test(sql)) {
      applySql(sql, handler);
      return;
    }

    handler.handle(Future.failedFuture("DDL не опознан: " + sql));
  }

  private void applySql(String sql, Handler<AsyncResult<Void>> handler) {
    adbDatabaseExecutor.executeUpdate(sql, executeResult -> {
      if (executeResult.succeeded()) {
        handler.handle(Future.succeededFuture());
      } else {
        handler.handle(Future.failedFuture(executeResult.cause()));
      }
    });
  }


  private static Predicate<String> predicate(String regex) {
    return sql -> Pattern.compile(regex, Pattern.CASE_INSENSITIVE).matcher(sql).find();
  }

  private void createTable(DdlRequest request, Handler<AsyncResult<Void>> handler) {
    metadataFactory.apply(request.getClassTable(), ar -> {
      if (ar.succeeded()) {
        if (!request.isCreateTopic()) {
          handler.handle(Future.succeededFuture());
          return;
        }
        kafkaTopicService.createOrReplace(getTopics(request.getClassTable()), ar2 -> {
          if (ar2.succeeded()) {
            handler.handle(Future.succeededFuture());
          } else {
            LOGGER.error("Ошибка генерации топиков в Kafka", ar2.cause());
            handler.handle(Future.failedFuture(ar2.cause()));
          }
        });
      } else {
        LOGGER.error("Ошибка исполнения запроса", ar.cause());
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  private void dropTable(DdlRequest request, Handler<AsyncResult<Void>> handler) {
    metadataFactory.purge(request.getClassTable(), ar -> {
      if (ar.succeeded()) {
        kafkaTopicService.delete(getTopics(request.getClassTable()), ar2 -> {
          if (ar2.succeeded()) {
            handler.handle(Future.succeededFuture());
          } else {
            LOGGER.error("Ошибка удаления топиков в Kafka", ar2.cause());
            handler.handle(Future.failedFuture(ar2.cause()));
          }
        });
      } else {
        LOGGER.error("Ошибка исполнения запроса", ar.cause());
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  private List<String> getTopics(ClassTable table) {
    KafkaAdminProperty properties = kafkaProperties.getAdmin();
    String adbUploadRq = String.format(properties.getAdbUploadRq(), table.getName(), table.getSchema());
    String adbUploadRs = String.format(properties.getAdbUploadRs(), table.getName(), table.getSchema());
    String adbUploadErr = String.format(properties.getAdbUploadErr(), table.getName(), table.getSchema());
    return Arrays.asList(adbUploadRq, adbUploadRs, adbUploadErr);
  }
}
