package ru.ibs.dtm.query.execution.plugin.adg.service.impl.ddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.configuration.kafka.KafkaAdminProperty;
import ru.ibs.dtm.common.configuration.kafka.KafkaConfig;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.plugin.adg.model.schema.SchemaReq;
import ru.ibs.dtm.query.execution.plugin.adg.service.*;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.DdlRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.DdlService;

import java.util.Arrays;
import java.util.List;

import static ru.ibs.dtm.query.execution.plugin.adg.constants.ColumnFields.ACTUAL_POSTFIX;
import static ru.ibs.dtm.query.execution.plugin.adg.constants.ColumnFields.HISTORY_POSTFIX;
import static ru.ibs.dtm.query.execution.plugin.adg.constants.Procedures.DROP_SPACE;

@Slf4j
@Service("adgDdlService")
public class AdgDdlService implements DdlService<Void> {

    private TtCartridgeProvider cartridgeProvider;
    private KafkaTopicService kafkaTopicService;
    private KafkaConfig kafkaProperties;
    private AvroSchemaGenerator schemaGenerator;
    private SchemaRegistryClient registryClient;
    private final QueryExecutorService executorService;

    @Autowired
    public AdgDdlService(TtCartridgeProvider cartridgeProvider, KafkaTopicService kafkaTopicService,
                         @Qualifier("coreKafkaProperties") KafkaConfig kafkaProperties, AvroSchemaGenerator schemaGenerator, SchemaRegistryClient registryClient, QueryExecutorService executorService) {
        this.cartridgeProvider = cartridgeProvider;
        this.kafkaTopicService = kafkaTopicService;
        this.kafkaProperties = kafkaProperties;
        this.schemaGenerator = schemaGenerator;
        this.registryClient = registryClient;
        this.executorService = executorService;
    }

    @Override
    public void execute(DdlRequestContext context, Handler<AsyncResult<Void>> handler) {

        switch (context.getDdlType()) {
            case DROP_TABLE:
                dropTable(context.getRequest(), handler);
                return;
            case CREATE_SCHEMA:
            case DROP_SCHEMA:
                handler.handle(Future.succeededFuture());
                return;
        }
        DdlRequest ddl = context.getRequest();
        cartridgeProvider.apply(ddl.getClassTable(), ar1 -> {
            if (ar1.succeeded()) {
                kafkaTopicService.createOrReplace(getTopics(ddl.getClassTable()), ar2 -> {
                    if (ar2.succeeded()) {
                        Schema schema = schemaGenerator.generate(ddl.getClassTable());
                        registryClient.register(getSubject(ddl.getClassTable()), new SchemaReq(schema.toString()), ar3 -> {
                            if (ar3.succeeded()) {
                                handler.handle(Future.succeededFuture());
                            } else {
                                handler.handle(Future.failedFuture(ar3.cause()));
                            }
                        });
                    } else {
                        handler.handle(Future.failedFuture(ar2.cause()));
                    }
                });
            } else {
                handler.handle(Future.failedFuture(ar1.cause()));
            }
        });
    }

    private void dropTable(DdlRequest ddl, Handler<AsyncResult<Void>> handler) {
        //удаление таблиц из базы
        dropSpacesFromDb(ddl.getClassTable(), dropResult -> {
            if (dropResult.succeeded()) {
                //удаление конфигов
                cartridgeProvider.delete(ddl.getClassTable(), deleteResult -> {
                    if (deleteResult.succeeded()) {
                        //удаление топиков
                        kafkaTopicService.delete(getTopics(ddl.getClassTable()), deleteTopicResult -> {
                            if (deleteTopicResult.succeeded()) {
                                //удаление из SchemaRegistry
                                registryClient.unregister(getSubject(ddl.getClassTable()), ar3 -> {
                                    if (ar3.succeeded()) {
                                        handler.handle(Future.succeededFuture());
                                    } else {
                                        handler.handle(Future.failedFuture(ar3.cause()));
                                    }
                                });
                            } else {
                                log.warn("При удалении топиков произошла ошибка", deleteTopicResult.cause());
                                handler.handle(Future.failedFuture(deleteTopicResult.cause()));
                            }
                        });
                    } else {
                        handler.handle(Future.failedFuture(deleteResult.cause()));
                    }
                });
            } else {
                handler.handle(Future.failedFuture(dropResult.cause()));
            }
        });
    }

    /**
     * Удаление таблиц из базы Tarantool (_actual и _history)
     *
     * @param classTable - описание таблицы
     * @param handler    - обработчик
     */
    private void dropSpacesFromDb(ClassTable classTable, Handler<AsyncResult<Void>> handler) {

        String actualTable = classTable.getName() + ACTUAL_POSTFIX;
        String historyTable = classTable.getName() + HISTORY_POSTFIX;

        executorService.executeProcedure(ar -> {
            if (ar.succeeded()) {
                log.debug("Удаление {} выполнено", actualTable);
                executorService.executeProcedure(
                        ar2 -> {
                            if (ar2.succeeded()) {
                                log.debug("Удаление {} выполнено", historyTable);
                                handler.handle(Future.succeededFuture());
                            } else {
                                log.error("Ошибка при удалении таблицы {}", historyTable);
                                handler.handle(Future.failedFuture(ar.cause()));
                            }
                        }, DROP_SPACE, historyTable);

            } else {
                log.error("Ошибка при удалении таблицы {}", actualTable);
            }
        }, DROP_SPACE, actualTable);

        //TODO вернуть после того , как функция drop_space_on_cluster будет корректно работать при параллельных вызовах

//    List<Future> futures = new ArrayList<>();
//    futures.add(Future.future(p -> executorService.executeProcedure(
//      ar -> {
//        if (ar.succeeded()) {
//          p.complete();
//        } else {
//          log.error("Ошибка при удалении таблицы {}", actualTable);
//          p.fail(ar.cause());
//        }
//      }, DROP_SPACE, actualTable)));
//
//    futures.add(Future.future(p -> executorService.executeProcedure(
//      ar -> {
//        if (ar.succeeded()) {
//          p.complete();
//        } else {
//          log.error("Ошибка при удалении таблицы {}", historyTable);
//          p.fail(ar.cause());
//        }
//      }, DROP_SPACE, historyTable)));
//
//    CompositeFuture.all(futures).setHandler(ar -> {
//      if (ar.succeeded()) {
//        handler.handle(Future.succeededFuture());
//      } else {
//        handler.handle(Future.failedFuture(ar.cause()));
//      }
//    });
    }

    private List<String> getTopics(ClassTable classTable) {
        KafkaAdminProperty properties = kafkaProperties.getKafkaAdminProperty();
        String adgUploadRq = String.format(properties.getUpload().getRequestTopic().get(SourceType.ADG.toString().toLowerCase()), classTable.getName(), classTable.getSchema());
        String adgUploadRs = String.format(properties.getUpload().getResponseTopic().get(SourceType.ADG.toString().toLowerCase()), classTable.getName(), classTable.getSchema());
        String adgUploadErr = String.format(properties.getUpload().getErrorTopic().get(SourceType.ADG.toString().toLowerCase()), classTable.getName(), classTable.getSchema());
        return Arrays.asList(adgUploadRq, adgUploadRs, adgUploadErr);
    }

    private String getSubject(ClassTable classTable) {
        return String.format(kafkaProperties.getKafkaAdminProperty().getUpload().getRequestTopic().get(SourceType.ADG.toString().toLowerCase()),
                classTable.getName(), classTable.getSchema()).replace(".", "-");
    }
}
