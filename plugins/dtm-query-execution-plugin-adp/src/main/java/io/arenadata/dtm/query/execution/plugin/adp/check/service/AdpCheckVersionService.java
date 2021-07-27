package io.arenadata.dtm.query.execution.plugin.adp.check.service;

import io.arenadata.dtm.common.version.VersionInfo;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckVersionRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckVersionService;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

import java.util.List;

@Service("adpCheckVersionService")
public class AdpCheckVersionService implements CheckVersionService {
    @Override
    public Future<List<VersionInfo>> checkVersion(CheckVersionRequest request) {
        return Future.failedFuture(new UnsupportedOperationException("Not implemented"));
    }
}
