package io.arenadata.dtm.query.execution.plugin.api.check;

import io.arenadata.dtm.query.execution.plugin.api.dto.PluginRequest;

import java.util.UUID;

public class CheckVersionRequest extends PluginRequest {

    public CheckVersionRequest(UUID requestId, String envName, String datamartMnemonic) {
        super(requestId, envName, datamartMnemonic);
    }
}
