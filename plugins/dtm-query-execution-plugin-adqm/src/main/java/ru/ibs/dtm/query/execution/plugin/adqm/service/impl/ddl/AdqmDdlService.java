package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.ddl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.common.configuration.kafka.KafkaAdminProperty;
import ru.ibs.dtm.common.configuration.kafka.KafkaConfig;
import ru.ibs.dtm.common.model.ddl.ClassTable;
import ru.ibs.dtm.common.reader.SourceType;
import ru.ibs.dtm.query.execution.plugin.adqm.factory.MetadataFactory;
import ru.ibs.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import ru.ibs.dtm.query.execution.plugin.adqm.service.KafkaTopicCreatorService;
import ru.ibs.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import ru.ibs.dtm.query.execution.plugin.api.service.ddl.DdlService;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service("adqmDdlService")
@Slf4j
public class AdqmDdlService implements DdlService<Void> {
    private final Map<SqlKind, DdlExecutor<Void>> ddlExecutors = new HashMap<>();

    @Override
    public void execute(DdlRequestContext context, Handler<AsyncResult<Void>> handler) {
        SqlNode query = context.getQuery();
        if (query == null) {
            handler.handle(Future.failedFuture("DdlRequestContext.query is null"));
            return;
        }

        if (ddlExecutors.containsKey(query.getKind())) {
            ddlExecutors.get(query.getKind()).execute(context, query.getKind().lowerName, handler);
        } else {
            handler.handle(Future.failedFuture(String.format("Unknown DDL %s", query)));
        }
    }

    @Override
    public void addExecutor(DdlExecutor<Void> executor) {
        ddlExecutors.put(executor.getSqlKind(), executor);
    }
}
