package io.arenadata.dtm.query.execution.plugin.adg.service.impl.check;

import io.arenadata.dtm.common.version.VersionInfo;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckVersionRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckVersionService;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

import java.util.List;

@Service("adgCheckVersionService")
public class AdgCheckVersionService implements CheckVersionService {
    @Override
    public Future<List<VersionInfo>> checkVersion(CheckVersionRequest request) {
        return Future.succeededFuture(null);
    }
}
