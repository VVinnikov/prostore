package io.arenadata.dtm.query.execution.plugin.api.service.mppw;

import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequest;
import io.vertx.core.Future;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MppwServiceImpl<T extends MppwExecutor> implements MppwService {
    private final Map<ExternalTableLocationType, MppwExecutor> executors;

    public MppwServiceImpl(List<T> executors) {
        this.executors = executors.stream()
                .collect(Collectors.toMap(MppwExecutor::getType, Function.identity()));
    }

    @Override
    public Future<QueryResult> execute(MppwRequest request) {
        return executors.get(request.getExternalTableLocationType()).execute(request);
    }
}
