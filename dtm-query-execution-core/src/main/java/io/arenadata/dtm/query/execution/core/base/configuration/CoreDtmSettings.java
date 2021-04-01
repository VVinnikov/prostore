package io.arenadata.dtm.query.execution.core.base.configuration;

import io.arenadata.dtm.common.configuration.core.DtmConfig;

import java.time.ZoneId;

public class CoreDtmSettings implements DtmConfig {

    private final ZoneId timeZone;

    public CoreDtmSettings(ZoneId timeZone) {
        this.timeZone = timeZone;
    }

    @Override
    public ZoneId getTimeZone() {
        return this.timeZone;
    }
}
