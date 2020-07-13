package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.mppw;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.api.request.MppwRequest;

@Component("adqmMppwStartRequestHandler")
@Slf4j
public class MppwStartRequestHandler implements MppwRequestHandler {

    @Override
    public Future<QueryResult> execute(MppwRequest request) {
        Promise<QueryResult> promise = Promise.promise();
        return promise.future();
    }
}
