package io.arenadata.dtm.status.monitor.version;

import io.arenadata.dtm.common.version.VersionInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.info.BuildProperties;
import org.springframework.stereotype.Component;

@Component
public class VersionServiceImpl implements VersionService {

    private final BuildProperties buildProperties;

    @Autowired
    public VersionServiceImpl(BuildProperties buildProperties) {
        this.buildProperties = buildProperties;
    }

    @Override
    public VersionInfo getVersionInfo() {
        return new VersionInfo(buildProperties.getName(), buildProperties.getVersion());
    }
}
