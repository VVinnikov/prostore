package io.arenadata.dtm.query.execution.plugin.api.service.check;

import io.arenadata.dtm.common.version.VersionInfo;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckVersionRequest;
import io.vertx.core.Future;

import java.util.List;

public interface CheckVersionService {
    Future<List<VersionInfo>> checkVersion(CheckVersionRequest request);
}
