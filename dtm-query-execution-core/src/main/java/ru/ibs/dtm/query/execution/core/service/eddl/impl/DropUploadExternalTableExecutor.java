package ru.ibs.dtm.query.execution.core.service.eddl.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.kafka.admin.ConsumerGroupListing;
import io.vertx.kafka.admin.KafkaAdminClient;
import org.jooq.generated.dtmservice.tables.records.UploadExternalTableRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.core.dao.ServiceDao;
import ru.ibs.dtm.query.execution.core.dto.eddl.DropUploadExternalTableQuery;
import ru.ibs.dtm.query.execution.core.dto.eddl.EddlAction;
import ru.ibs.dtm.query.execution.core.dto.eddl.EddlQuery;
import ru.ibs.dtm.query.execution.core.dto.edml.UploadExtTableRecord;
import ru.ibs.dtm.query.execution.core.service.eddl.EddlExecutor;

import java.util.List;
import java.util.Set;

@Component
public class DropUploadExternalTableExecutor implements EddlExecutor {

    private final ServiceDao serviceDao;
    private final KafkaAdminClient kafkaAdminClient;

    @Autowired
    public DropUploadExternalTableExecutor(ServiceDao serviceDao, KafkaAdminClient kafkaAdminClient) {
        this.serviceDao = serviceDao;
        this.kafkaAdminClient = kafkaAdminClient;
    }

    @Override
    public void execute(EddlQuery query, Handler<AsyncResult<Void>> asyncResultHandler) {
        DropUploadExternalTableQuery castQuery;
        try {
            castQuery = (DropUploadExternalTableQuery) query;
        } catch (Exception e) {
            asyncResultHandler.handle(Future.failedFuture(e));
            return;
        }
        executeInternal(castQuery, asyncResultHandler);
    }

    @Override
    public EddlAction getAction() {
        return EddlAction.DROP_UPLOAD_EXTERNAL_TABLE;
    }

    private void executeInternal(DropUploadExternalTableQuery query, Handler<AsyncResult<Void>> asyncResultHandler) {
        //TODO добавить функционал по коммиту сообщений в кафке
        serviceDao.findUploadExternalTable(query.getSchemaName(), query.getTableName(), ar -> {
            if (ar.succeeded()) {
                UploadExtTableRecord uploadRecord = ar.result();
                kafkaAdminClient.listTopics(cg -> {
                    if (cg.succeeded()){
                        Set<String> result = cg.result();

                    }
                });
            }

        });
        serviceDao.dropUploadExternalTable(query, asyncResultHandler);
    }
}
