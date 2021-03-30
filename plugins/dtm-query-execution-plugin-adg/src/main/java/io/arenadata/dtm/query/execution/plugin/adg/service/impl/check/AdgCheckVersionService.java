package io.arenadata.dtm.query.execution.plugin.adg.service.impl.check;

import io.arenadata.dtm.common.version.VersionInfo;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckVersionRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckVersionService;
import io.vertx.core.Future;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service("adgCheckVersionService")
public class AdgCheckVersionService implements CheckVersionService {

    private final AdgCartridgeClient adgCartridgeClient;

    @Autowired
    public AdgCheckVersionService(AdgCartridgeClient adgCartridgeClient) {
        this.adgCartridgeClient = adgCartridgeClient;
    }

    @Override
    public Future<List<VersionInfo>> checkVersion(CheckVersionRequest request) {
        return adgCartridgeClient.getCheckVersions();
    }
}
