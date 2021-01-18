package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.dml.DmlType;
import io.arenadata.dtm.query.execution.core.dto.dml.DmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.service.dml.DmlExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.dml.DmlService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.EnumMap;
import java.util.Map;

@Slf4j
@Service("coreDmlService")
public class DmlServiceImpl implements DmlService<QueryResult> {
    private final Map<DmlType, DmlExecutor<QueryResult>> executorMap;

    public DmlServiceImpl() {
        this.executorMap = new EnumMap<>(DmlType.class);
    }

    @Override
    public Future<QueryResult> execute(DmlRequestContext context) {
        return getExecutor(context)
                .compose(executor -> executor.execute(context));
    }

    private Future<DmlExecutor<QueryResult>> getExecutor(DmlRequestContext context) {
        return Future.future(promise -> {
            final DmlExecutor<QueryResult> dmlExecutor = executorMap.get(context.getType());
            if (dmlExecutor != null) {
                promise.complete(dmlExecutor);
            } else {
                promise.fail(new DtmException(
                        String.format("Couldn't find dml executor for query kind %s",
                                context.getQuery().getKind())));
            }
        });
    }

    @Override
    public void addExecutor(DmlExecutor<QueryResult> executor) {
        executorMap.put(executor.getType(), executor);
    }
}
