package ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.executor.AdbMppwRequestExecutor;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.MppwRequest;
import ru.ibs.dtm.query.execution.plugin.api.service.MppwKafkaService;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component("adbMppwKafkaService")
public class AdbMppwKafkaService implements MppwKafkaService<QueryResult> {

    private static final Map<LoadType, AdbMppwRequestExecutor> mppwExecutors = new HashMap<>();

    public AdbMppwKafkaService(@Qualifier("adbMppwStartRequestExecutor") AdbMppwRequestExecutor mppwStartExecutor,
                               @Qualifier("adbMppwStopRequestExecutor") AdbMppwRequestExecutor mppwStopExecutor) {
        mppwExecutors.put(LoadType.START, mppwStartExecutor);
        mppwExecutors.put(LoadType.STOP, mppwStopExecutor);
    }

    @Override
    public void execute(MppwRequestContext context, Handler<AsyncResult<QueryResult>> asyncHandler) {
        try {
            MppwRequest request = context.getRequest();
            if (request == null) {
                asyncHandler.handle(Future.failedFuture("MppwRequest should not be null"));
                return;
            }
            final LoadType loadType = LoadType.valueOf(context.getRequest().getIsLoadStart());
            mppwExecutors.get(loadType).execute(context).onComplete(asyncHandler);
        } catch (Exception e) {
            asyncHandler.handle(Future.failedFuture(e));
        }
    }

    private enum LoadType {
        START(true),
        STOP(false);

        LoadType(boolean b) {
            this.load = b;
        }

        static LoadType valueOf(boolean b) {
            return b ? START : STOP;
        }

        private final boolean load;
    }
}
