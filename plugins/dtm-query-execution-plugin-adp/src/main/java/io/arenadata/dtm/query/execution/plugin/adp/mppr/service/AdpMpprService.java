package io.arenadata.dtm.query.execution.plugin.adp.mppr.service;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.mppr.MpprService;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

@Service("adpMpprService")
public class AdpMpprService implements MpprService {
    @Override
    public Future<QueryResult> execute(MpprRequest request) {
        return Future.failedFuture(new UnsupportedOperationException("Not implemented"));
    }
}
