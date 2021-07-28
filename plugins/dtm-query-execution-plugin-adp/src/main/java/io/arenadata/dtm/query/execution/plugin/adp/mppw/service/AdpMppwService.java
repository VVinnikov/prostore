package io.arenadata.dtm.query.execution.plugin.adp.mppw.service;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.mppw.MppwService;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

@Service("adpMppwService")
public class AdpMppwService implements MppwService {
    @Override
    public Future<QueryResult> execute(MppwRequest request) {
        return Future.failedFuture(new UnsupportedOperationException("Not implemented"));
    }
}
