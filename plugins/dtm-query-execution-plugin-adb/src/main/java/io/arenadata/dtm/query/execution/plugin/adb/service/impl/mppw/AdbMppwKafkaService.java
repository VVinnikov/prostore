package io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.executor.AdbMppwRequestExecutor;
import io.arenadata.dtm.query.execution.plugin.api.exception.MppwDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.MppwRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.MppwKafkaService;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component("adbMppwKafkaService")
public class AdbMppwKafkaService implements MppwKafkaService<QueryResult> {

    private static final Map<LoadType, AdbMppwRequestExecutor> mppwExecutors = new HashMap<>();

    @Autowired
    public AdbMppwKafkaService(@Qualifier("adbMppwStartRequestExecutor") AdbMppwRequestExecutor mppwStartExecutor,
                               @Qualifier("adbMppwStopRequestExecutor") AdbMppwRequestExecutor mppwStopExecutor) {
        mppwExecutors.put(LoadType.START, mppwStartExecutor);
        mppwExecutors.put(LoadType.STOP, mppwStopExecutor);
    }

    @Override
    public Future<QueryResult> execute(MppwRequestContext context) {
        return Future.future(promise -> {
            MppwRequest request = context.getRequest();
            if (request == null) {
                promise.fail(new MppwDatasourceException("MppwRequest should not be null"));
                return;
            }
            final LoadType loadType = LoadType.valueOf(context.getRequest().getIsLoadStart());
            mppwExecutors.get(loadType).execute(context)
                    .onComplete(promise);
        });
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
