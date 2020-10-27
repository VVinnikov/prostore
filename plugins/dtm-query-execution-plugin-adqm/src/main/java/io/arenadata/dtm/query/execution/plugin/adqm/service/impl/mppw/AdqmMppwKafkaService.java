package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppw;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.MppwKafkaService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service("adqmMppwKafkaService")
public class AdqmMppwKafkaService implements MppwKafkaService<QueryResult> {
    private enum LoadType {
        START(true),
        FINISH(false);

        LoadType(boolean b) {
            this.load = b;
        }

        static LoadType valueOf(boolean b) {
            return b ? START : FINISH;
        }

        private boolean load;
    }

    private static final Map<LoadType, MppwRequestHandler> handlers = new HashMap<>();

    @Autowired
    public AdqmMppwKafkaService(
            @Qualifier("adqmMppwStartRequestHandler") MppwRequestHandler startRequestHandler,
            @Qualifier("adqmMppwFinishRequestHandler") MppwRequestHandler finishRequestHandler
    ) {
        handlers.put(LoadType.START, startRequestHandler);
        handlers.put(LoadType.FINISH, finishRequestHandler);
    }

    @Override
    public void execute(MppwRequestContext context, Handler<AsyncResult<QueryResult>> asyncResultHandler) {
        log.debug("mppw start");
        MppwRequest request = context.getRequest();
        if (request == null) {
            asyncResultHandler.handle(Future.failedFuture("MppwRequest should not be null"));
            return;
        }

        LoadType loadType = LoadType.valueOf(request.getIsLoadStart());
        handlers.get(loadType).execute(request).onComplete(asyncResultHandler);
    }

}