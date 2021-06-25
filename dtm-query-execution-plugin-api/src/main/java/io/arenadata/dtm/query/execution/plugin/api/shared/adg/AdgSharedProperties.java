package io.arenadata.dtm.query.execution.plugin.api.shared.adg;

import lombok.Data;

@Data
public class AdgSharedProperties {
    private final String server;
    private final String user;
    private final String password;
    private final Long connectTimeout;
    private final Long readTimeout;
    private final Long requestTimeout;
}
