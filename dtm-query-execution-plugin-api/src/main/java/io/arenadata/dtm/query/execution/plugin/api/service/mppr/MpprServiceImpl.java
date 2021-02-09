package io.arenadata.dtm.query.execution.plugin.api.service.mppr;

import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequest;
import io.vertx.core.Future;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MpprServiceImpl<T extends MpprExecutor> implements MpprService {
    private final Map<ExternalTableLocationType, MpprExecutor> executors;

    public MpprServiceImpl(List<T> executors) {
        this.executors = executors.stream()
                .collect(Collectors.toMap(MpprExecutor::getType, Function.identity()));
    }

    @Override
    public Future<QueryResult> execute(MpprRequest request) {
        return executors.get(request.getExternalTableLocationType()).execute(request);
    }
}
