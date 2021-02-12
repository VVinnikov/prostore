package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppw;

import io.arenadata.dtm.common.model.ddl.ExternalTableFormat;
import io.arenadata.dtm.common.model.ddl.ExternalTableLocationType;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adqm.service.AdqmMppwExecutor;
import io.arenadata.dtm.query.execution.plugin.api.exception.MppwDatasourceException;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service("adqmMppwKafkaService")
public class AdqmMppwKafkaService implements AdqmMppwExecutor {
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
            @Qualifier("adqmMppwFinishRequestHandler") MppwRequestHandler finishRequestHandler) {
        handlers.put(LoadType.START, startRequestHandler);
        handlers.put(LoadType.FINISH, finishRequestHandler);
    }

    @Override
    public Future<QueryResult> execute(MppwRequest request) {
        return Future.future(promise -> {
            if (request == null) {
                promise.fail(new MppwDatasourceException("MppwRequest should not be null"));
                return;
            }
            if (request.getUploadMetadata().getFormat() != ExternalTableFormat.AVRO) {
                promise.fail(new MppwDatasourceException(String.format("Format %s not implemented",
                        request.getUploadMetadata().getFormat())));
            }
            LoadType loadType = LoadType.valueOf(request.getIsLoadStart());
            log.debug("Mppw {}", loadType);
            handlers.get(loadType).execute((MppwKafkaRequest) request)
                    .onComplete(promise);
        });
    }

    @Override
    public ExternalTableLocationType getType() {
        return ExternalTableLocationType.KAFKA;
    }

}
